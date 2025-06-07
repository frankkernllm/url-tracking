// File: netlify/functions/store-attribution.js
// Netlify Function: store-attribution.js
// Stores attribution data for later lookup during purchases

// Simple in-memory storage (for demo - replace with database in production)
let attributionStore = new Map();

const handler = async (event, context) => {
  // Handle CORS preflight
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'POST, GET, OPTIONS'
      }
    };
  }

  if (event.httpMethod === 'POST') {
    // Store attribution data
    try {
      const attributionData = JSON.parse(event.body);
      
      console.log('📊 Storing attribution data:', JSON.stringify(attributionData, null, 2));
      
      // Create multiple lookup keys for flexible matching
      const keys = [
        `ip:${attributionData.ip_address}`,
        `session:${attributionData.session_id}`,
        `timestamp:${Math.floor(new Date(attributionData.timestamp).getTime() / 60000)}` // Round to minute
      ];
      
      // Store with multiple keys for different lookup strategies
      keys.forEach(key => {
        attributionStore.set(key, {
          ...attributionData,
          stored_at: new Date().toISOString()
        });
      });
      
      // Clean up old entries (keep last 1000 for memory management)
      if (attributionStore.size > 1000) {
        const entries = Array.from(attributionStore.entries()).slice(-800);
        attributionStore.clear();
        entries.forEach(([key, value]) => attributionStore.set(key, value));
      }
      
      console.log(`✅ Attribution stored with ${keys.length} lookup keys`);
      
      return {
        statusCode: 200,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ 
          success: true, 
          message: 'Attribution data stored successfully',
          keys: keys.length,
          session_id: attributionData.session_id
        })
      };
      
    } catch (error) {
      console.error('❌ Error storing attribution:', error);
      return {
        statusCode: 500,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ error: error.message })
      };
    }
  }
  
  if (event.httpMethod === 'GET') {
    // Lookup attribution data
    const { ip, session_id, timestamp, email } = event.queryStringParameters || {};
    
    try {
      let attributionData = null;
      
      // Try different lookup strategies
      if (session_id) {
        attributionData = attributionStore.get(`session:${session_id}`);
        console.log(`🔍 Session lookup for ${session_id}:`, !!attributionData);
      }
      
      if (!attributionData && ip) {
        attributionData = attributionStore.get(`ip:${ip}`);
        console.log(`🔍 IP lookup for ${ip}:`, !!attributionData);
      }
      
      if (!attributionData && timestamp) {
        // Look for attribution data within 5 minutes of the timestamp
        const targetMinute = Math.floor(new Date(timestamp).getTime() / 60000);
        for (let i = -5; i <= 5; i++) {
          const key = `timestamp:${targetMinute + i}`;
          attributionData = attributionStore.get(key);
          if (attributionData) {
            console.log(`🔍 Timestamp lookup found match at ${targetMinute + i}`);
            break;
          }
        }
      }
      
      if (!attributionData) {
        // Fallback: find most recent entry for this IP
        const ipEntries = Array.from(attributionStore.entries())
          .filter(([key]) => key.startsWith(`ip:${ip}`))
          .sort(([,a], [,b]) => new Date(b.timestamp) - new Date(a.timestamp));
        
        if (ipEntries.length > 0) {
          attributionData = ipEntries[0][1];
          console.log(`🔍 Recent IP fallback found data`);
        }
      }
      
      return {
        statusCode: 200,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ 
          found: !!attributionData,
          data: attributionData,
          lookup_params: { ip, session_id, timestamp, email }
        })
      };
      
    } catch (error) {
      return {
        statusCode: 500,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ error: error.message })
      };
    }
  }
  
  return {
    statusCode: 405,
    headers: { 'Access-Control-Allow-Origin': '*' },
    body: 'Method not allowed'
  };
};

module.exports = { handler };
