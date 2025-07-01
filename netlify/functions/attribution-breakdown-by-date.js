// File: netlify/functions/attribution-breakdown-by-date.js
// Detailed breakdown of conversions and attribution by date

const handler = async (event, context) => {
  console.log('📅 Starting date-by-date attribution breakdown...');
  
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Content-Type': 'application/json'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

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

    console.log(`📅 Analyzing conversions from ${startDate.toISOString()} to ${endDate.toISOString()}`);

    // Get conversion keys
    let allKeys = [];
    
    try {
      let cursor = '0';
      do {
        const result = await redis(`scan/${cursor}/match/conversions:*/count/1000`);
        if (result.result && result.result[1]) {
          cursor = result.result[0];
          const keys = result.result[1];
          allKeys = allKeys.concat(keys);
          if (allKeys.length > 10000) break;
        } else {
          break;
        }
      } while (cursor !== '0');
      
      console.log(`🔍 Found ${allKeys.length} total conversion keys`);
    } catch (error) {
      console.error('❌ SCAN failed:', error.message);
    }

    // Process conversions and group by date
    const dateBreakdown = {};
    const dailyStats = {};
    
    // Initialize date structure
    for (let d = new Date(startDate); d <= endDate; d.setDate(d.getDate() + 1)) {
      const dateKey = d.toISOString().split('T')[0]; // YYYY-MM-DD format
      dateBreakdown[dateKey] = {
        total_conversions: 0,
        attributed_conversions: 0,
        unattributed_conversions: 0,
        attribution_methods: {},
        attribution_sources: {},
        sample_conversions: []
      };
    }
    
    for (const key of allKeys) {
      try {
        const conversionResult = await redis(`get/${key}`);
        const conversionData = conversionResult.result;
        if (!conversionData) continue;
        
        let conversion;
        try {
          conversion = typeof conversionData === 'string' ? JSON.parse(conversionData) : conversionData;
        } catch (parseError) {
          continue;
        }
        
        if (!conversion.timestamp) continue;
        
        const conversionTimestamp = new Date(conversion.timestamp).getTime();
        
        // Check if conversion is in our date range
        if (conversionTimestamp >= startTimestamp && conversionTimestamp <= endTimestamp) {
          const conversionDate = new Date(conversion.timestamp);
          const dateKey = conversionDate.toISOString().split('T')[0];
          
          if (dateBreakdown[dateKey]) {
            dateBreakdown[dateKey].total_conversions++;
            
            // Store sample conversion info
            if (dateBreakdown[dateKey].sample_conversions.length < 3) {
              dateBreakdown[dateKey].sample_conversions.push({
                email: conversion.email,
                timestamp: conversion.timestamp,
                order_total: conversion.order_total || 0,
                attributed: conversion.attribution_found || false,
                method: conversion.attribution_method || 'none',
                source: conversion.source || 'direct',
                landing_page: conversion.landing_page ? conversion.landing_page.substring(0, 50) + '...' : 'none'
              });
            }
            
            if (conversion.attribution_found) {
              dateBreakdown[dateKey].attributed_conversions++;
              
              // Track attribution methods
              const method = conversion.attribution_method || 'unknown';
              dateBreakdown[dateKey].attribution_methods[method] = 
                (dateBreakdown[dateKey].attribution_methods[method] || 0) + 1;
              
              // Track attribution sources
              const source = conversion.source || 'direct';
              dateBreakdown[dateKey].attribution_sources[source] = 
                (dateBreakdown[dateKey].attribution_sources[source] || 0) + 1;
            } else {
              dateBreakdown[dateKey].unattributed_conversions++;
            }
          }
        }
      } catch (error) {
        console.log(`⚠️ Error processing conversion ${key}:`, error.message);
      }
    }
    
    // Calculate daily statistics
    let totalConversions = 0;
    let totalAttributed = 0;
    
    for (const dateKey in dateBreakdown) {
      const dayData = dateBreakdown[dateKey];
      const attributionRate = dayData.total_conversions > 0 ? 
        Math.round((dayData.attributed_conversions / dayData.total_conversions) * 100) : 0;
      
      dailyStats[dateKey] = {
        date: dateKey,
        total_conversions: dayData.total_conversions,
        attributed_conversions: dayData.attributed_conversions,
        unattributed_conversions: dayData.unattributed_conversions,
        attribution_rate: attributionRate,
        top_attribution_method: Object.keys(dayData.attribution_methods).length > 0 ? 
          Object.keys(dayData.attribution_methods).reduce((a, b) => 
            dayData.attribution_methods[a] > dayData.attribution_methods[b] ? a : b) : 'none',
        top_source: Object.keys(dayData.attribution_sources).length > 0 ?
          Object.keys(dayData.attribution_sources).reduce((a, b) => 
            dayData.attribution_sources[a] > dayData.attribution_sources[b] ? a : b) : 'direct'
      };
      
      totalConversions += dayData.total_conversions;
      totalAttributed += dayData.attributed_conversions;
    }
    
    // Overall summary
    const overallSummary = {
      total_conversions: totalConversions,
      total_attributed: totalAttributed,
      overall_attribution_rate: totalConversions > 0 ? Math.round((totalAttributed / totalConversions) * 100) : 0,
      date_range: {
        start: startDate.toISOString().split('T')[0],
        end: endDate.toISOString().split('T')[0]
      }
    };
    
    // Find best and worst days
    const daysWithData = Object.values(dailyStats).filter(day => day.total_conversions > 0);
    const bestDay = daysWithData.reduce((best, current) => 
      current.attribution_rate > best.attribution_rate ? current : best, { attribution_rate: -1 });
    const worstDay = daysWithData.reduce((worst, current) => 
      current.attribution_rate < worst.attribution_rate ? current : worst, { attribution_rate: 101 });

    console.log('📊 Date breakdown analysis complete');

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        message: 'Attribution breakdown by date completed',
        results: {
          overall_summary: overallSummary,
          daily_stats: dailyStats,
          detailed_breakdown: dateBreakdown,
          insights: {
            best_attribution_day: bestDay,
            worst_attribution_day: worstDay,
            total_days_analyzed: Object.keys(dateBreakdown).length,
            days_with_conversions: daysWithData.length
          }
        }
      })
    };

  } catch (error) {
    console.error('❌ Analysis error:', error);
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
