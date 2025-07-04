// Enhanced Recovery Function - Uses analytics-flexible.js comprehensive scanning
// File: netlify/functions/comprehensive-recovery.js

const handler = async (event, context) => {
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: corsHeaders, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return {
      statusCode: 405,
      headers: corsHeaders,
      body: JSON.stringify({ error: 'Method not allowed' })
    };
  }

  try {
    console.log('🔍 Enhanced Recovery Function - Using Comprehensive Scanning');
    
    const data = JSON.parse(event.body);
    const { email, ip, conversion_ip, checkoutview } = data;

    // Extract both IPs like staged-recovery does
    const pageviewIP = ip;
    const conversionIP = checkoutview?.pageviewcheckout?.pageview?.ip || conversion_ip;
    
    console.log(`📧 Email: ${email}`);
    console.log(`📍 Pageview IP: ${pageviewIP}`);
    console.log(`🛒 Conversion IP: ${conversionIP}`);

    // Initialize Redis
    const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
    const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;

    if (!redisUrl || !redisToken) {
      throw new Error('Redis configuration missing');
    }

    const redis = async (command) => {
      const response = await fetch(`${redisUrl}/${command}`, {
        headers: { Authorization: `Bearer ${redisToken}` }
      });
      return response.json();
    };

    // Use comprehensive scanning to find attribution
    const attributionResult = await findAttributionComprehensive(
      redis, 
      pageviewIP, 
      conversionIP
    );

    return {
      statusCode: 200,
      headers: corsHeaders,
      body: JSON.stringify({
        success: true,
        email: email,
        pageview_ip: pageviewIP,
        conversion_ip: conversionIP,
        attribution_found: attributionResult.found,
        attribution_method: attributionResult.method,
        attribution_score: attributionResult.score,
        landing_page: attributionResult.landing_page,
        source: attributionResult.source,
        utm_campaign: attributionResult.utm_campaign,
        search_details: attributionResult.search_details
      })
    };

  } catch (error) {
    console.error('❌ Enhanced recovery error:', error);
    return {
      statusCode: 500,
      headers: corsHeaders,
      body: JSON.stringify({
        success: false,
        error: error.message
      })
    };
  }
};

// Comprehensive attribution finding using analytics-flexible.js methods
async function findAttributionComprehensive(redis, pageviewIP, conversionIP) {
  console.log('🔍 Starting comprehensive attribution search...');
  
  const result = {
    found: false,
    method: 'none',
    score: 0,
    landing_page: null,
    source: null,
    utm_campaign: null,
    search_details: {
      direct_lookup_attempted: false,
      comprehensive_scan_attempted: false,
      redis_keys_checked: 0,
      attribution_keys_found: 0
    }
  };

  // Step 1: Try direct lookups first (faster)
  console.log('📍 Step 1: Trying direct IP lookups...');
  result.search_details.direct_lookup_attempted = true;
  
  const ipsToCheck = [pageviewIP, conversionIP].filter(Boolean);
  
  for (const ip of ipsToCheck) {
    const encodedIP = ip.replace(/:/g, '_'); // IPv6 encoding
    const directKey = `attribution_ip_${encodedIP}`;
    
    try {
      console.log(`🔍 Checking direct key: ${directKey}`);
      const directResult = await redis(`get/${directKey}`);
      
      if (directResult.result) {
        // Get the main attribution data
        const mainKey = directResult.result;
        const attributionData = await redis(`get/${mainKey}`);
        
        if (attributionData.result) {
          const parsed = JSON.parse(attributionData.result);
          result.found = true;
          result.method = 'direct_ip_lookup';
          result.score = ip === pageviewIP ? 280 : 260;
          result.landing_page = parsed.landing_page;
          result.source = parsed.source;
          result.utm_campaign = parsed.utm_campaign;
          result.search_details.redis_keys_checked += 2;
          
          console.log(`✅ Direct lookup success for IP: ${ip}`);
          return result;
        }
      }
      result.search_details.redis_keys_checked += 1;
    } catch (error) {
      console.warn(`⚠️ Direct lookup failed for ${ip}:`, error);
    }
  }

  // Step 2: Comprehensive Redis scanning (like analytics-flexible.js)
  console.log('🔍 Step 2: Starting comprehensive Redis scanning...');
  result.search_details.comprehensive_scan_attempted = true;
  
  try {
    // Get all attribution keys using comprehensive scanning
    const attributionKeys = await getComprehensiveAttributionKeys(redis);
    result.search_details.attribution_keys_found = attributionKeys.length;
    
    console.log(`📊 Found ${attributionKeys.length} attribution keys to check`);
    
    // Search through all attribution data for IP matches
    const batchSize = 50;
    for (let i = 0; i < attributionKeys.length; i += batchSize) {
      const batch = attributionKeys.slice(i, i + batchSize);
      
      console.log(`📦 Checking batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(attributionKeys.length/batchSize)}`);
      
      const batchResults = await Promise.all(
        batch.map(async (key) => {
          try {
            const data = await redis(`get/${key}`);
            if (data.result) {
              result.search_details.redis_keys_checked += 1;
              const parsed = JSON.parse(data.result);
              
              // Check if this attribution data matches our IPs
              if (parsed.ip_address === pageviewIP || parsed.ip_address === conversionIP) {
                return {
                  key: key,
                  data: parsed,
                  matched_ip: parsed.ip_address
                };
              }
            }
            return null;
          } catch (error) {
            return null;
          }
        })
      );
      
      // Check if we found a match in this batch
      const match = batchResults.find(r => r !== null);
      if (match) {
        result.found = true;
        result.method = 'comprehensive_scan_match';
        result.score = match.matched_ip === pageviewIP ? 280 : 260;
        result.landing_page = match.data.landing_page;
        result.source = match.data.source;
        result.utm_campaign = match.data.utm_campaign;
        
        console.log(`✅ Comprehensive scan success! Matched IP: ${match.matched_ip}`);
        return result;
      }
      
      // Small delay to avoid overwhelming Redis
      if (i + batchSize < attributionKeys.length) {
        await new Promise(resolve => setTimeout(resolve, 50));
      }
    }
    
    console.log('❌ No attribution found in comprehensive scan');
    
  } catch (error) {
    console.error('❌ Comprehensive scanning failed:', error);
  }

  return result;
}

// Comprehensive attribution key scanning (copied from analytics-flexible.js)
async function getComprehensiveAttributionKeys(redis) {
  console.log('🔍 Starting comprehensive attribution key scanning...');
  
  let allAttributionKeys = [];
  
  try {
    // Scan for attribution_* pattern
    let cursor = '0';
    
    do {
      const result = await redis(`scan/${cursor}/match/attribution_*/count/1000`);
      if (result.result && result.result[1]) {
        cursor = result.result[0];
        const keys = result.result[1];
        allAttributionKeys = allAttributionKeys.concat(keys);
        
        if (allAttributionKeys.length % 1000 === 0) {
          console.log(`📊 Scanning progress: ${allAttributionKeys.length} keys found`);
        }
      } else {
        break;
      }
    } while (cursor !== '0' && allAttributionKeys.length < 10000);
    
    console.log(`✅ Attribution key scan complete: ${allAttributionKeys.length} keys found`);
    return allAttributionKeys;
    
  } catch (error) {
    console.error('❌ Attribution key scanning failed:', error);
    return [];
  }
}

module.exports = { handler };
