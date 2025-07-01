// File: netlify/functions/attribution-recovery-june-analysis.js
// MINIMAL CLEAN VERSION - No hardcoded values anywhere

const handler = async (event, context) => {
  console.log('Starting June 23-30 conversion analysis...');
  
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Content-Type': 'application/json'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  // Use environment variable for API key
  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  const validApiKey = process.env.OJOY_API_KEY;
  
  if (!apiKey || apiKey !== validApiKey) {
    return {
      statusCode: 401,
      headers,
      body: JSON.stringify({ error: 'Unauthorized' })
    };
  }

  try {
    const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
    const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
    
    const redis = async (command) => {
      const response = await fetch(`${redisUrl}/${command}`, {
        headers: { Authorization: `Bearer ${redisToken}` }
      });
      return response.json();
    };

    // Date range
    const startDate = new Date('2025-06-23T00:00:00.000Z');
    const endDate = new Date('2025-06-30T23:59:59.999Z');
    const startTimestamp = startDate.getTime();
    const endTimestamp = endDate.getTime();

    // Get conversion keys
    const allKeysResult = await redis('keys/conversion_*');
    const allKeys = allKeysResult.result || [];
    
    const conversionsInRange = [];
    
    for (const key of allKeys) {
      try {
        const conversionResult = await redis(`get/${key}`);
        const conversionData = conversionResult.result;
        if (!conversionData) continue;
        
        const conversion = JSON.parse(conversionData);
        const conversionTimestamp = new Date(conversion.timestamp).getTime();
        
        if (conversionTimestamp >= startTimestamp && conversionTimestamp <= endTimestamp) {
          conversionsInRange.push({ key, data: conversion });
        }
      } catch (error) {
        continue;
      }
    }

    const totalConversions = conversionsInRange.length;
    let currentlyAttributed = 0;
    let recoverySuccessful = 0;

    for (const { data: conversion } of conversionsInRange) {
      if (conversion.attribution_found) {
        currentlyAttributed++;
      }
      // Recovery logic would go here
    }

    const results = {
      total_conversions_analyzed: totalConversions,
      currently_attributed: currentlyAttributed,
      recovery_successful: recoverySuccessful,
      attribution_rate: Math.round((currentlyAttributed / totalConversions) * 100)
    };

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        message: 'Analysis completed',
        results: results
      })
    };

  } catch (error) {
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Analysis failed', 
        details: error.message 
      })
    };
  }
};

module.exports = { handler };
