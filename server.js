const express = require('express');
const axios = require('axios');
const { Kafka } = require('kafkajs');
const http = require('http');
const { Server } = require("socket.io");
const cors = require('cors');
const path = require('path');
const { Pool } = require('pg');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));

// --- CONFIGURATION ---
const REST_API_KEY = process.env.RAIL_API_KEY || 'EhPYIKPzBrWdoIqeA6u1hGc54eJSCcZxiGGgGqfGSwkwuGVQ';
const REST_BASE_URL = 'https://api1.raildata.org.uk/1010-live-departure-board---staff-version1_0/LDBSVWS/api/20220120/GetDepBoardWithDetails';

const KAFKA_CONFIG = {
    brokers: ['pkc-z3p1v0.europe-west2.gcp.confluent.cloud:9092'],
    ssl: true,
    sasl: {
        mechanism: 'plain',
        username: 'GZSSPQNWCOTEVH4R',
        password: 'cfltITE1NBLRmcGJsqzwXQpkRoYLreyhf2fZwqxNZXvAInxsEy1T2rd+S0+GEDaQ'
    },
    groupId: 'SC-de021cfb-7f0b-4996-8f27-bf8c68793fcd'
};

// --- POSTGRESQL CONNECTION (Digital Ocean) ---
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

// --- REST API: Get Departure Board ---
app.get('/api/departures/:crs', async (req, res) => {
    const crs = req.params.crs.toUpperCase();
    const now = new Date();
    const timeString = now.toISOString().replace(/[-:]/g, '').split('.')[0]; 
    const url = `${REST_BASE_URL}/${crs}/${timeString}`;

    try {
        console.log(`Fetching REST data for ${crs}...`);
        const response = await axios.get(url, {
            headers: {
                'x-apikey': REST_API_KEY,
                'Accept': 'application/json'
            }
        });
        res.json(response.data);
    } catch (error) {
        console.error("REST API Error:", error.message);
        res.status(500).json({ error: "Failed to fetch data" });
    }
});

// --- JAX PREDICTIONS API ---
app.get('/api/predictions', async (req, res) => {
    try {
        const result = await pool.query(
            'SELECT forecast_date, predicted_minutes, stress_coefficient, data_type, rmse, updated_at FROM jax_predictions ORDER BY forecast_date'
        );
        res.json({ status: 'ok', predictions: result.rows });
    } catch (error) {
        console.error("Predictions DB Error:", error.message);
        res.status(500).json({ status: 'error', message: error.message });
    }
});

// --- LIVE SNAPSHOT API (from Streamlit) ---
app.get('/api/live-snapshot', async (req, res) => {
    try {
        const result = await pool.query(
            'SELECT station, delay_minutes, status, total_delayed_trains, total_delay_minutes, updated_at FROM live_snapshot ORDER BY id'
        );
        
        let totalDelayed = 0;
        let totalDelay = 0;
        let updated = null;
        
        if (result.rows.length > 0) {
            totalDelayed = result.rows[0].total_delayed_trains;
            totalDelay = result.rows[0].total_delay_minutes;
            updated = result.rows[0].updated_at;
        }
        
        res.json({
            status: 'ok',
            stations: result.rows,
            total_delayed_trains: totalDelayed,
            total_delay_minutes: totalDelay,
            updated: updated
        });
    } catch (error) {
        console.error("Live Snapshot DB Error:", error.message);
        res.status(500).json({ status: 'error', message: error.message });
    }
});

// --- JOURNEY PLANNER API ---
app.get('/api/plan/:date', async (req, res) => {
    try {
        const result = await pool.query(
            'SELECT forecast_date, predicted_minutes, stress_coefficient, data_type, rmse FROM jax_predictions WHERE forecast_date = $1',
            [req.params.date]
        );
        
        if (result.rows.length === 0) {
            return res.status(404).json({ status: 'error', message: 'No prediction for this date. The JAX model forecasts 30 days ahead.' });
        }
        
        const row = result.rows[0];
        const stress = parseFloat(row.stress_coefficient);
        const minutes = parseFloat(row.predicted_minutes);
        
        let confidence, recommendation, color;
        
        if (stress < 0.2) {
            confidence = 'HIGH'; color = '#22c55e';
            recommendation = 'Excellent day to travel. Minimal delays expected on the Manchester-Euston route.';
        } else if (stress < 0.4) {
            confidence = 'GOOD'; color = '#84cc16';
            recommendation = 'Good conditions. Minor delays possible but unlikely to affect your journey.';
        } else if (stress < 0.6) {
            confidence = 'MODERATE'; color = '#eab308';
            recommendation = 'Some delays expected. Allow 15-20 extra minutes for your journey.';
        } else if (stress < 0.8) {
            confidence = 'LOW'; color = '#f97316';
            recommendation = 'Significant delays likely. Consider travelling earlier or later if possible.';
        } else {
            confidence = 'POOR'; color = '#ef4444';
            recommendation = 'Severe disruption expected. Consider alternative transport or rescheduling.';
        }
        
        res.json({
            status: 'ok',
            date: row.forecast_date,
            predicted_minutes: Math.round(minutes * 10) / 10,
            stress_coefficient: Math.round(stress * 10000) / 10000,
            data_type: row.data_type,
            rmse: Math.round(parseFloat(row.rmse) * 100) / 100,
            confidence,
            recommendation,
            color
        });
    } catch (error) {
        console.error("Plan API Error:", error.message);
        res.status(500).json({ status: 'error', message: error.message });
    }
});

// --- HEALTH CHECK ---
app.get('/health', (req, res) => {
    res.json({ status: 'ok', time: new Date().toISOString() });
});

// --- KAFKA: Live Stream Consumer ---
const runKafka = async () => {
    const kafka = new Kafka({
        clientId: 'rail-app-client',
        brokers: KAFKA_CONFIG.brokers,
        ssl: KAFKA_CONFIG.ssl,
        sasl: KAFKA_CONFIG.sasl
    });

    const consumer = kafka.consumer({ groupId: KAFKA_CONFIG.groupId });

    try {
        await consumer.connect();
        console.log("✅ Connected to Rail Data Kafka");
        
        await consumer.subscribe({ 
            topic: 'prod-1010-Darwin-Train-Information-Push-Port-IIII2_0-JSON',
            fromBeginning: false 
        });

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const rawData = message.value.toString();
                io.emit('kafka-message', rawData); 
            },
        });
    } catch (err) {
        console.error("Kafka Connection Error:", err);
    }
};

runKafka();

// Start Server
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});