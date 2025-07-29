// Debug Batch Multi-Touch Attribution - Find Conversion Dates
// Path: netlify/functions/debug-batch-attribution.js
// Purpose: Debug what conversion dates are available

exports.handler = async (event, context) => {
  context.callbackWaitsForEmptyEventLoop = false;
  
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
    'Access-Control-Allow-Methods': 'POST, OPTIONS'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  try {
    const redis = initializeRedis();
    const startTime = Date.now();
    
    // Parse request body
    const requestData = JSON.parse(event.body || '{}');
    const { target_date = "2025-06-10" } = requestData;
    
    console.log(`🔍 Debug: Looking for conversion data for date: ${target_date}`);
    
    // Step 1: Check if the specific date index exists
    console.log('📅 Step 1: Checking specific date index...');
    const dateIndexKey = `conversion_index_v1_date:${target_date}`;
    const dateResult = await redis(`get/${dateIndexKey}`, 3000);
    
    let specificDateInfo = {
      key: dateIndexKey,
      exists: false,
      conversions: 0,
      sample_conversion: null
    };
    
    if (dateResult?.result) {
      try {
        const dateIndex = JSON.parse(decodeURIComponent(dateResult.result));
        specificDateInfo = {
          key: dateIndexKey,
          exists: true,
          conversions: dateIndex.conversion_count || 0,
          sample_conversion: dateIndex.conversions?.[0] || null,
          total_revenue: dateIndex.total_revenue || 0,
          created_at: dateIndex.created_at
        };
        console.log(`✅ Found date index with ${specificDateInfo.conversions} conversions`);
      } catch (parseError) {
        console.log(`❌ Error parsing date index: ${parseError.message}`);
      }
    } else {
      console.log(`❌ Date index not found: ${dateIndexKey}`);
    }
    
    // Step 2: Scan for all available date indexes
    console.log('🔍 Step 2: Scanning for all date indexes...');
    let cursor = '0';
    const allDateIndexes = [];
    let scanAttempts = 0;
    
    do {
      try {
        const scanResult = await redis(`scan/${cursor}/match/conversion_index_v1_date:*/count/100`);
        
        if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
          break;
        }
        
        cursor = scanResult.result[0];
        const keys = scanResult.result[1] || [];
        
        console.log(`📦 Scan attempt ${scanAttempts + 1}: Found ${keys.length} date index keys`);
        
        // Get info for each date index
        for (const key of keys) {
          try {
            const result = await redis(`get/${key}`, 1500);
            if (result?.result) {
              const index = JSON.parse(decodeURIComponent(result.result));
              const dateFromKey = key.replace('conversion_index_v1_date:', '');
              
              allDateIndexes.push({
                date: dateFromKey,
                key: key,
                conversions: index.conversion_count || 0,
                created_at: index.created_at,
                total_revenue: index.total_revenue || 0
              });
            }
          } catch (error) {
            console.log(`⚠️ Error reading date index ${key}: ${error.message}`);
          }
        }
        
        scanAttempts++;
        if (scanAttempts >= 20) break; // Safety limit
        
      } catch (scanError) {
        console.log(`❌ Scan error: ${scanError.message}`);
        break;
      }
    } while (cursor !== '0');
    
    // Sort by date
    allDateIndexes.sort((a, b) => new Date(b.date) - new Date(a.date));
    
    // Step 3: Check batch progress for the target date
    console.log('📊 Step 3: Checking batch progress...');
    const progressKey = `batch_attribution_progress:date:${target_date}`;
    const progressResult = await redis(`get/${progressKey}`, 2000);
    
    let batchProgress = null;
    if (progressResult?.result) {
      try {
        batchProgress = JSON.parse(decodeURIComponent(progressResult.result));
      } catch (error) {
        console.log(`⚠️ Error parsing batch progress: ${error.message}`);
      }
    }
    
    // Step 4: Look for email indexes to see if conversions exist elsewhere
    console.log('📧 Step 4: Checking for email conversion indexes...');
    cursor = '0';
    let emailIndexCount = 0;
    let emailSampleDates = [];
    let emailScanAttempts = 0;
    
    do {
      try {
        const scanResult = await redis(`scan/${cursor}/match/conversion_index_v1_email:*/count/50`);
        
        if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
          break;
        }
        
        cursor = scanResult.result[0];
        const keys = scanResult.result[1] || [];
        emailIndexCount += keys.length;
        
        // Sample a few email indexes to see their dates
        if (emailSampleDates.length < 10 && keys.length > 0) {
          for (const key of keys.slice(0, 3)) {
            try {
              const result = await redis(`get/${key}`, 1000);
              if (result?.result) {
                const index = JSON.parse(decodeURIComponent(result.result));
                if (index.conversions && index.conversions.length > 0) {
                  const sampleConversion = index.conversions[0];
                  const conversionDate = new Date(sampleConversion.timestamp).toISOString().split('T')[0];
                  emailSampleDates.push({
                    email: index.email,
                    conversion_date: conversionDate,
                    timestamp: sampleConversion.timestamp
                  });
                }
              }
            } catch (error) {
              // Skip errors
            }
          }
        }
        
        emailScanAttempts++;
        if (emailScanAttempts >= 10) break; // Limit scan
        
      } catch (scanError) {
        console.log(`❌ Email scan error: ${scanError.message}`);
        break;
      }
    } while (cursor !== '0' && emailScanAttempts < 10);
    
    const totalTime = Date.now() - startTime;
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        debug_info: {
          target_date: target_date,
          processing_time_ms: totalTime,
          
          specific_date_check: specificDateInfo,
          
          all_available_dates: {
            total_date_indexes_found: allDateIndexes.length,
            dates: allDateIndexes.slice(0, 20), // Show first 20
            total_conversions_across_all_dates: allDateIndexes.reduce((sum, d) => sum + d.conversions, 0)
          },
          
          batch_progress_check: {
            progress_key: progressKey,
            has_progress: !!batchProgress,
            progress_data: batchProgress
          },
          
          email_indexes_check: {
            total_email_indexes_found: emailIndexCount,
            sample_conversion_dates: emailSampleDates
          },
          
          diagnosis: specificDateInfo.exists 
            ? `✅ Date index exists with ${specificDateInfo.conversions} conversions`
            : `❌ Date index missing for ${target_date}. Found ${allDateIndexes.length} other dates.`,
            
          recommendations: specificDateInfo.exists && specificDateInfo.conversions === 0
            ? ["Date index exists but has 0 conversions", "Check if conversions were filtered out during indexing"]
            : !specificDateInfo.exists 
            ? ["Date index doesn't exist", "Run extract-conversion-data-v2.js to rebuild indexes", `Available dates: ${allDateIndexes.slice(0, 5).map(d => d.date).join(', ')}`]
            : ["Date index exists with conversions", "Issue might be elsewhere in batch attribution logic"]
        }
      })
    };
    
  } catch (error) {
    console.error('❌ Debug failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Debug failed', 
        message: error.message 
      })
    };
  }
};

function initializeRedis() {
  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
  
  return async (command, timeoutMs = 3000) => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    
    try {
      const response = await fetch(`${redisUrl}/${command}`, {
        headers: { 
          Authorization: `Bearer ${redisToken}`,
          'Content-Type': 'application/json'
        },
        signal: controller.signal
      });
      
      clearTimeout(timeoutId);
      
      if (!response.ok) {
        throw new Error(`Redis error: ${response.status}`);
      }
      
      return await response.json();
    } catch (error) {
      clearTimeout(timeoutId);
      throw error;
    }
  };
}
