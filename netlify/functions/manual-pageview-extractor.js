// manual-pageview-extractor.js - Direct pageview extraction with multiple patterns
// Path: netlify/functions/manual-pageview-extractor.js

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, X-API-Key, Authorization',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
  'Access-Control-Max-Age': '86400',
  'Content-Type': 'application/json'
};

function createCorsResponse(statusCode, body) {
  return {
    statusCode,
    headers: CORS_HEADERS,
    body: typeof body === 'string' ? body : JSON.stringify(body)
  };
}

exports.handler = async (event, context) => {
  if (event.httpMethod === 'OPTIONS') {
    return createCorsResponse(200, { message: 'CORS preflight successful' });
  }

  const redis = (path) => {
    const url = `${process.env.UPSTASH_REDIS_REST_URL}/${path}`;
    return fetch(url, {
      headers: { 'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
    }).then(r => r.json());
  };

  try {
    const { 
      start_date = "2025-07-11", 
      end_date = "2025-07-12",
      patterns = ["attribution_*", "attribution:*", "pageview_*", "*attribution*"],
      force_build_indexes = true 
    } = JSON.parse(event.body || '{}');
    
    console.log(`🔍 Manual pageview extraction for ${start_date} to ${end_date}`);
    
    const startTime = new Date(start_date + "T00:00:00.000Z").getTime();
    const endTime = new Date(end_date + "T23:59:59.999Z").getTime();
    
    const results = {
      extraction_summary: {
        patterns_tried: [],
        total_keys_scanned: 0,
        valid_pageviews_found: 0,
        pageviews_in_date_range: 0,
        unique_ips: new Set(),
        date_range: { start_date, end_date, start_time: startTime, end_time: endTime }
      },
      pageviews: [],
      indexes_created: {
        ip_indexes: 0,
        date_indexes: 0
      }
    };
    
    // Try each pattern to find pageview data
    for (const pattern of patterns) {
      console.log(`🔍 Trying pattern: ${pattern}`);
      
      const patternResult = {
        pattern: pattern,
        keys_found: 0,
        valid_pageviews: 0,
        pageviews_in_range: 0
      };
      
      let cursor = '0';
      let iterations = 0;
      const maxIterations = 10;
      
      do {
        const scanResult = await redis(`scan/${cursor}/match/${pattern}/count/500`);
        
        if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
          break;
        }
        
        cursor = scanResult.result[0];
        const keys = scanResult.result[1] || [];
        patternResult.keys_found += keys.length;
        results.extraction_summary.total_keys_scanned += keys.length;
        
        // Process keys in batches
        const batchSize = 25;
        for (let i = 0; i < keys.length; i += batchSize) {
          const batch = keys.slice(i, i + batchSize);
          
          const batchPromises = batch.map(async (key) => {
            try {
              // Skip lookup keys, only process main data keys
              if (key.includes('_ip_') || key.includes('_session_') || key.includes('_fp_') || 
                  key.includes('_webgl_') || key.includes('_screen_') || key.includes('_geo_') ||
                  key.includes('pageview_index_')) {
                return null;
              }
              
              const data = await redis(`get/${key}`);
              if (data?.result) {
                const parsed = JSON.parse(data.result);
                
                // Validate this is pageview data
                if (parsed.timestamp && parsed.ip_address && 
                    (parsed.landing_page || parsed.url || parsed.page_url)) {
                  
                  patternResult.valid_pageviews++;
                  
                  // Check if in date range
                  const pageviewTime = new Date(parsed.timestamp).getTime();
                  if (pageviewTime >= startTime && pageviewTime <= endTime) {
                    patternResult.pageviews_in_range++;
                    results.extraction_summary.unique_ips.add(parsed.ip_address);
                    
                    return {
                      timestamp: parsed.timestamp,
                      ip_address: parsed.ip_address,
                      landing_page: parsed.landing_page || parsed.url || parsed.page_url,
                      source: parsed.source || 'unknown',
                      utm_campaign: parsed.utm_campaign,
                      utm_medium: parsed.utm_medium,
                      utm_source: parsed.utm_source,
                      utm_term: parsed.utm_term,
                      utm_content: parsed.utm_content,
                      referrer_url: parsed.referrer_url,
                      session_id: parsed.session_id,
                      device_signature: parsed.canvas_fingerprint,
                      redis_key: key,
                      found_via_pattern: pattern
                    };
                  }
                }
              }
            } catch (parseError) {
              // Skip invalid data
            }
            return null;
          });
          
          const batchResults = await Promise.all(batchPromises);
          const validResults = batchResults.filter(result => result !== null);
          results.pageviews.push(...validResults);
        }
        
        iterations++;
      } while (cursor !== '0' && iterations < maxIterations);
      
      results.extraction_summary.patterns_tried.push(patternResult);
      console.log(`✅ Pattern ${pattern}: ${patternResult.keys_found} keys, ${patternResult.valid_pageviews} pageviews, ${patternResult.pageviews_in_range} in range`);
    }
    
    // Update summary
    results.extraction_summary.valid_pageviews_found = results.pageviews.length;
    results.extraction_summary.pageviews_in_date_range = results.pageviews.length;
    results.extraction_summary.unique_ips = Array.from(results.extraction_summary.unique_ips);
    
    // Sort pageviews by timestamp
    results.pageviews.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
    
    // Build indexes if requested and we found pageviews
    if (force_build_indexes && results.pageviews.length > 0) {
      console.log(`🏗️ Building indexes for ${results.pageviews.length} pageviews...`);
      
      // Build IP indexes
      const ipGroups = {};
      results.pageviews.forEach(pv => {
        const encodedIP = pv.ip_address.replace(/:/g, '_');
        if (!ipGroups[encodedIP]) {
          ipGroups[encodedIP] = [];
        }
        ipGroups[encodedIP].push(pv);
      });
      
      // Store IP indexes
      for (const [encodedIP, ipPageviews] of Object.entries(ipGroups)) {
        try {
          ipPageviews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
          
          const ipKey = `pageview_index_ip:${encodedIP}`;
          const indexData = {
            ip_address: ipPageviews[0].ip_address,
            pageview_count: ipPageviews.length,
            latest_timestamp: ipPageviews[0].timestamp,
            earliest_timestamp: ipPageviews[ipPageviews.length - 1].timestamp,
            pageviews: ipPageviews.slice(0, 30), // Store up to 30 most recent
            created_at: new Date().toISOString(),
            created_by: 'manual_extractor'
          };
          
          await redis(`setex/${ipKey}/7200/${encodeURIComponent(JSON.stringify(indexData))}`); // 2 hours TTL
          results.indexes_created.ip_indexes++;
        } catch (indexError) {
          console.warn(`⚠️ Failed to create IP index for ${encodedIP}:`, indexError.message);
        }
      }
      
      console.log(`✅ Created ${results.indexes_created.ip_indexes} IP indexes`);
    }
    
    return createCorsResponse(200, {
      success: true,
      extraction_results: results,
      message: results.pageviews.length > 0 ? 
        `Successfully extracted ${results.pageviews.length} pageviews and created ${results.indexes_created.ip_indexes} IP indexes` :
        'No pageviews found in the specified date range',
      next_steps: results.pageviews.length > 0 ? [
        'Test fast-analytics endpoint',
        'Check customer journey mapping',
        'Verify dashboard shows pageview data'
      ] : [
        'Check if pageview data exists for different date ranges',
        'Verify store-attribution.js is storing data correctly',
        'Check Redis for any attribution-related keys'
      ]
    });

  } catch (error) {
    console.error('❌ Manual pageview extraction error:', error);
    return createCorsResponse(500, {
      success: false,
      error: 'Manual pageview extraction failed',
      message: error.message
    });
  }
};
