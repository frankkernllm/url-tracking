// File: netlify/functions/attribution-recovery-june-analysis.js
// COMPLETELY REWRITTEN - Safe results object construction

const handler = async (event, context) => {
  console.log('🚀 Starting June 23-30 conversion analysis...');
  
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

    // Initialize results object early to avoid undefined errors
    let results = {
      total_conversions_analyzed: 0,
      currently_attributed: 0,
      currently_unattributed: 0,
      attribution_rate: 0,
      recovery_successful: 0,
      sample_conversions: [],
      redis_keys_found: 0,
      date_range: {
        start: '2025-06-23T00:00:00.000Z',
        end: '2025-06-30T23:59:59.999Z'
      }
    };

    // Date range
    const startDate = new Date('2025-06-23T00:00:00.000Z');
    const endDate = new Date('2025-06-30T23:59:59.999Z');
    const startTimestamp = startDate.getTime();
    const endTimestamp = endDate.getTime();

    console.log(`📅 Analyzing conversions from ${startDate.toISOString()} to ${endDate.toISOString()}`);

    // Get conversion keys using CORRECT pattern: conversions:*
    console.log('🔍 Scanning for conversions using CORRECT pattern: conversions:*');
    
    let allKeys = [];
    
    try {
      let cursor = '0';
      let iterations = 0;
      
      do {
        const result = await redis(`scan/${cursor}/match/conversions:*/count/1000`);
        if (result.result && result.result[1]) {
          cursor = result.result[0];
          const keys = result.result[1];
          allKeys = allKeys.concat(keys);
          iterations++;
          
          console.log(`✅ Batch ${iterations}: Found ${keys.length} conversion keys (total: ${allKeys.length}, cursor: ${cursor})`);
          
          if (allKeys.length > 10000) {
            console.warn('⚠️ Breaking after 10,000 keys for memory safety');
            break;
          }
          
          if (iterations % 10 === 0) {
            await new Promise(resolve => setTimeout(resolve, 50));
          }
        } else {
          console.log(`⚠️ No results from SCAN at cursor ${cursor}, stopping iteration`);
          break;
        }
      } while (cursor !== '0');
      
      console.log(`🎯 SCAN COMPLETE: ${allKeys.length} conversion keys found using pattern 'conversions:*'`);
      results.redis_keys_found = allKeys.length;
      
    } catch (error) {
      console.error('❌ SCAN failed:', error.message);
    }

    // Process conversions for June 23-30 analysis
    let totalConversions = 0;
    let currentlyAttributed = 0;
    let sampleConversions = [];
    
    console.log('🔍 Processing conversions for date range analysis...');
    
    for (const key of allKeys) {
      try {
        const conversionResult = await redis(`get/${key}`);
        const conversionData = conversionResult.result;
        if (!conversionData) continue;
        
        let conversion;
        try {
          conversion = typeof conversionData === 'string' ? JSON.parse(conversionData) : conversionData;
        } catch (parseError) {
          console.log(`⚠️ Failed to parse conversion data from ${key}`);
          continue;
        }
        
        if (!conversion.timestamp) continue;
        
        const conversionTimestamp = new Date(conversion.timestamp).getTime();
        
        // Check if conversion is in our date range
        if (conversionTimestamp >= startTimestamp && conversionTimestamp <= endTimestamp) {
          totalConversions++;
          
          if (conversion.attribution_found) {
            currentlyAttributed++;
          }
          
          // Store sample for debugging
          if (sampleConversions.length < 5) {
            sampleConversions.push(key.substring(0, 50));
          }
          
          console.log(`📧 Conversion ${totalConversions}: ${conversion.email} - ${conversion.timestamp} - ${conversion.attribution_found ? 'ATTRIBUTED' : 'UNATTRIBUTED'}`);
        }
      } catch (error) {
        console.log(`⚠️ Error processing conversion ${key}:`, error.message);
      }
    }
    
    // Update results object with final values
    results.total_conversions_analyzed = totalConversions;
    results.currently_attributed = currentlyAttributed;
    results.currently_unattributed = totalConversions - currentlyAttributed;
    results.attribution_rate = totalConversions > 0 ? Math.round((currentlyAttributed/totalConversions)*100) : 0;
    results.sample_conversions = sampleConversions;

    console.log('📊 FINAL ANALYSIS RESULTS:');
    console.log(`Total Conversions: ${results.total_conversions_analyzed}`);
    console.log(`Currently Attributed: ${results.currently_attributed} (${results.attribution_rate}%)`);
    console.log(`Redis Keys Found: ${results.redis_keys_found}`);
    console.log(`Sample Keys: ${results.sample_conversions.join(', ')}`);

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        message: 'June 23-30 conversion analysis completed',
        results: results
      })
    };

  } catch (error) {
    console.error('❌ Analysis error:', error);
    
    // Return safe fallback results even on error
    const fallbackResults = {
      total_conversions_analyzed: 0,
      currently_attributed: 0,
      currently_unattributed: 0,
      attribution_rate: 0,
      recovery_successful: 0,
      error_message: error.message
    };
    
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        success: false,
        error: 'Analysis failed', 
        details: error.message,
        results: fallbackResults
      })
    };
  }
};

module.exports = { handler };
