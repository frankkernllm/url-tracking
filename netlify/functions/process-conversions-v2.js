// Continuous Conversion Processor - Fast Query V2
// Path: netlify/functions/process-conversions-v2.js
// Purpose: Process ALL conversions one at a time using fast query system

exports.handler = async (event, context) => {
  context.callbackWaitsForEmptyEventLoop = false;
  
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'POST, GET, OPTIONS'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  try {
    console.log('🔄 Starting continuous conversion processing (Fast Query V2)...');
    const startTime = Date.now();
    
    // Step 1: Get all conversions from analytics
    const analyticsData = await fetchAnalyticsData();
    console.log(`📊 Analytics data: ${analyticsData.conversions?.length || 0} total conversions`);
    
    // Step 2: Filter for unattributed conversions
    const allConversions = analyticsData.conversions || [];
    const unattributedConversions = getUnattributedConversions(allConversions);
    console.log(`🎯 Found ${unattributedConversions.length} unattributed conversions`);
    
    // Step 3: Filter out already processed conversions
    const unprocessedConversions = await filterUnprocessedConversions(unattributedConversions);
    console.log(`📋 Found ${unprocessedConversions.length} unprocessed conversions`);
    
    if (unprocessedConversions.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          message: `🎉 All conversions processed! Total: ${allConversions.length}, Unattributed: ${unattributedConversions.length}, All processed with Fast Query V2.`,
          results: {
            total_conversions: allConversions.length,
            unattributed: unattributedConversions.length,
            unprocessed: 0,
            processed_this_run: 0,
            status: 'ALL_PROCESSED_V2'
          }
        })
      };
    }
    
    // Step 4: Process the FIRST unprocessed conversion
    const conversionToProcess = unprocessedConversions[0];
    console.log(`\n🔬 Processing conversion: [PRIVACY PROTECTED]`);
    console.log(`   ⏰ Time: ${conversionToProcess.timestamp}`);
    console.log(`   📍 IP: ${conversionToProcess.ip_address || 'N/A'}`);
    
    // Step 5: Process using the fast query V2 system
    const processingResult = await processConversionWithFastQuery(conversionToProcess);
    
    // Step 6: Mark as processed
    await markAsProcessedV2(conversionToProcess, processingResult);
    
    // Step 7: Update global progress
    await updateGlobalProgress(allConversions.length, unattributedConversions.length, unprocessedConversions.length - 1, processingResult.success);
    
    const totalTime = Date.now() - startTime;
    const remainingCount = unprocessedConversions.length - 1;
    
    console.log(`\n🏁 Processing complete:`);
    console.log(`   ✅ Success: ${processingResult.success ? 'YES' : 'NO'}`);
    console.log(`   ⚡ Time: ${totalTime}ms`);
    console.log(`   📊 Remaining: ${remainingCount} conversions`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        processed_conversion: {
          email: '[PRIVACY_PROTECTED]',
          timestamp: conversionToProcess.timestamp,
          attribution_found: processingResult.success,
          attribution_method: processingResult.method,
          processing_time_ms: processingResult.processing_time_ms,
          query_performance: processingResult.query_performance
        },
        progress: {
          total_conversions: allConversions.length,
          unattributed_total: unattributedConversions.length,
          unattributed_remaining: remainingCount,
          processed_this_run: 1,
          processing_time_ms: totalTime,
          status: remainingCount > 0 ? 'MORE_TO_PROCESS' : 'ALL_PROCESSED_V2'
        },
        continue_processing: {
          has_more: remainingCount > 0,
          continue_command: remainingCount > 0 ? 
            'curl -X POST https://trackingojoy.netlify.app/.netlify/functions/process-conversions-v2' : 
            null,
          estimated_time_remaining: remainingCount > 0 ? 
            `${Math.round(remainingCount * (totalTime / 1000))} seconds if run continuously` : 
            null
        },
        performance_stats: {
          avg_processing_time_ms: totalTime,
          system_version: 'fast_query_v2',
          pageview_dataset_size: '18k+ pageviews with 5.6k IP indexes'
        }
      })
    };

  } catch (error) {
    console.error('❌ Continuous processing error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({
        error: 'Continuous processing failed',
        message: error.message,
        system_version: 'fast_query_v2'
      })
    };
  }
};

// Fetch analytics data (past 7 days to get recent conversions)
async function fetchAnalyticsData() {
  console.log('📊 Fetching analytics data...');
  
  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(endDate.getDate() - 7);
  
  // Ensure we don't go before June 11, 2025 (when data starts)
  const earliestDate = new Date('2025-06-11');
  if (startDate < earliestDate) {
    startDate.setTime(earliestDate.getTime());
  }
  
  const startDateStr = startDate.toISOString().split('T')[0];
  const endDateStr = endDate.toISOString().split('T')[0];
  
  console.log(`📅 Date range: ${startDateStr} to ${endDateStr}`);
  
  const params = new URLSearchParams();
  params.append('start_date', startDateStr);
  params.append('end_date', endDateStr);
  
  const apiUrl = `https://trackingojoy.netlify.app/.netlify/functions/analytics?${params}`;
  
  const response = await fetch(apiUrl, {
    headers: {
      'X-API-Key': process.env.OJOY_API_KEY
    }
  });
  
  if (!response.ok) {
    throw new Error(`Analytics API failed: ${response.status} ${response.statusText}`);
  }
  
  const data = await response.json();
  console.log(`✅ Analytics data loaded: ${data.conversions?.length || 0} conversions, ${data.page_views?.length || 0} pageviews`);
  
  return data;
}

// Get ONLY unattributed conversions
function getUnattributedConversions(allConversions) {
  const unattributed = allConversions.filter(conv => {
    const hasNoAttribution = !conv.landing_page || 
                            conv.landing_page === '' || 
                            conv.landing_page === 'NO ATTRIBUTION' ||
                            conv.landing_page === null ||
                            conv.landing_page === undefined ||
                            conv.landing_page === 'null';
    return hasNoAttribution;
  });
  
  // Sort by timestamp DESCENDING (newest first)
  const sortedUnattributed = unattributed.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
  
  console.log(`🔍 Found ${sortedUnattributed.length} unattributed conversions out of ${allConversions.length} total`);
  
  if (sortedUnattributed.length > 0) {
    console.log('📋 Unattributed conversions (newest first):');
    sortedUnattributed.slice(0, 3).forEach((conv, index) => {
      console.log(`   ${index + 1}. [PRIVACY PROTECTED] | ${conv.timestamp}`);
    });
    if (sortedUnattributed.length > 3) {
      console.log(`   ... and ${sortedUnattributed.length - 3} more`);
    }
  }
  
  return sortedUnattributed;
}

// Filter out conversions already processed with V2
async function filterUnprocessedConversions(unattributedConversions) {
  const unprocessedConversions = [];
  let alreadyProcessedV2Count = 0;
  
  for (const conversion of unattributedConversions) {
    const processedKey = `processed_v2:${conversion.email}:${conversion.timestamp}`;
    
    try {
      const processedData = await redisRequest('get', processedKey);
      
      if (processedData) {
        alreadyProcessedV2Count++;
        console.log(`   ⏭️ Skipping [PRIVACY PROTECTED] - already processed with V2`);
      } else {
        unprocessedConversions.push(conversion);
      }
    } catch (error) {
      // If we can't check status, assume unprocessed
      unprocessedConversions.push(conversion);
    }
  }
  
  console.log(`📊 Filtering results:`);
  console.log(`   ✅ Already processed with V2: ${alreadyProcessedV2Count}`);
  console.log(`   🎯 Available for processing: ${unprocessedConversions.length}`);
  
  return unprocessedConversions;
}

// Process conversion using Fast Query V2 system
async function processConversionWithFastQuery(conversion) {
  const processStartTime = Date.now();
  
  try {
    console.log('⚡ Processing with Fast Query V2 system...');
    
    // Call the staged recovery V2 system
    const response = await fetch('https://trackingojoy.netlify.app/.netlify/functions/staged-recovery-v2/stage-recovery', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': process.env.OJOY_API_KEY
      },
      body: JSON.stringify({
        email: conversion.email,
        timestamp: conversion.timestamp,
        order_id: conversion.order_id || `order_${Date.now()}`,
        pageview_ip: conversion.ip_address,
        conversion_ip: conversion.ip_address,
        source_file: 'continuous_processor_v2'
      }),
      signal: AbortSignal.timeout(10000) // 10 second timeout
    });
    
    if (!response.ok) {
      throw new Error(`Staged recovery failed: ${response.status} ${response.statusText}`);
    }
    
    const result = await response.json();
    
    const processingTime = Date.now() - processStartTime;
    
    console.log(`   ✅ Fast Query V2 result: ${result.attribution_found ? 'FOUND' : 'NOT FOUND'}`);
    console.log(`   ⚡ Processing time: ${processingTime}ms`);
    
    if (result.attribution_found) {
      console.log(`   🎯 Method: ${result.proposed_changes?.source?.proposed || 'unknown'}`);
      console.log(`   📍 Landing page: ${result.proposed_changes?.landing_page?.proposed || 'unknown'}`);
    }
    
    return {
      success: result.attribution_found,
      method: result.attribution_found ? 'fast_query_v2' : 'no_attribution_found',
      processing_time_ms: processingTime,
      query_performance: result.query_performance || {},
      staging_result: result,
      recovery_id: result.recovery_id || null
    };
    
  } catch (error) {
    const processingTime = Date.now() - processStartTime;
    console.error(`❌ Fast Query V2 processing error: ${error.message}`);
    
    return {
      success: false,
      method: 'processing_error',
      processing_time_ms: processingTime,
      error: error.message
    };
  }
}

// Mark conversion as processed with V2
async function markAsProcessedV2(conversion, processingResult) {
  try {
    const processedKey = `processed_v2:${conversion.email}:${conversion.timestamp}`;
    const processedData = {
      email: conversion.email,
      timestamp: conversion.timestamp,
      processed_at: new Date().toISOString(),
      system: 'fast_query_v2_continuous',
      attribution_found: processingResult.success,
      recovery_id: processingResult.recovery_id,
      processing_time_ms: processingResult.processing_time_ms
    };
    
    // Set with 30-day expiration
    await redisRequest('setex', processedKey, 2592000, JSON.stringify(processedData)); // 30 days
    console.log(`   ✅ Marked as processed with V2`);
  } catch (error) {
    console.log(`   ⚠️ Could not mark as processed: ${error.message}`);
  }
}

// Update global progress tracking
async function updateGlobalProgress(totalConversions, unattributedTotal, remaining, attributionFound) {
  try {
    const progressKey = 'continuous_processing_v2_progress';
    const progressData = await redisRequest('get', progressKey);
    
    let progress = {
      total_processed_v2: 0,
      attribution_found: 0,
      no_attribution: 0,
      started_at: new Date().toISOString(),
      last_updated: new Date().toISOString(),
      system_version: 'fast_query_v2_continuous'
    };
    
    if (progressData) {
      progress = JSON.parse(progressData);
    }
    
    progress.total_processed_v2++;
    progress.last_updated = new Date().toISOString();
    progress.total_conversions = totalConversions;
    progress.unattributed_total = unattributedTotal;
    progress.unattributed_remaining = remaining;
    
    if (attributionFound) {
      progress.attribution_found++;
    } else {
      progress.no_attribution++;
    }
    
    await redisRequest('setex', progressKey, 86400, JSON.stringify(progress)); // 24 hours
    
    if (progress.total_processed_v2 % 5 === 0) {
      const attributionRate = progress.total_processed_v2 > 0 ? 
        ((progress.attribution_found / progress.total_processed_v2) * 100).toFixed(1) : '0.0';
      console.log(`🔄 V2 Progress: ${progress.total_processed_v2} processed, ${progress.attribution_found} attributed (${attributionRate}%)`);
    }
    
  } catch (error) {
    console.log('⚠️ Error updating progress:', error.message);
  }
}

// Redis request helper
async function redisRequest(command, ...args) {
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  
  if (!url || !token) {
    throw new Error('Missing Redis configuration');
  }
  
  let response;
  
  try {
    if ((command.toLowerCase() === 'set' || command.toLowerCase() === 'setex') && args.length >= 2) {
      response = await fetch(url, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify([command, ...args])
      });
    } else {
      const encodedArgs = args.map(arg => encodeURIComponent(arg));
      const requestUrl = `${url}/${command}/${encodedArgs.join('/')}`;
      
      response = await fetch(requestUrl, {
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        }
      });
    }
    
    if (!response.ok) {
      if (response.status === 404) {
        return null;
      }
      throw new Error(`Redis request failed: ${response.status} ${response.statusText}`);
    }
    
    const data = await response.json();
    return data.result;
    
  } catch (error) {
    throw error;
  }
}
