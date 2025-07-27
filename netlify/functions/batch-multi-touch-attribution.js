// Batch Multi-Touch Attribution Engine - Phase 2
// Path: netlify/functions/batch-multi-touch-attribution.js
// Purpose: Process multiple conversions for comprehensive attribution analysis

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

  if (event.httpMethod !== 'POST') {
    return {
      statusCode: 405,
      headers,
      body: JSON.stringify({ error: 'Method not allowed' })
    };
  }

  try {
    const redis = initializeRedis();
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max
    
    // Parse request body
    const requestData = JSON.parse(event.body || '{}');
    const { query_type, date, emails, limit = 50, resume_from } = requestData;
    
    if (!query_type) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({ 
          error: 'Missing required field: query_type (date, emails, or all)' 
        })
      };
    }
    
    console.log(`🚀 Starting batch multi-touch attribution: ${query_type}`);
    
    // Load existing progress or start fresh
    const progressKey = `batch_attribution_progress:${query_type}:${date || 'all'}`;
    const existingProgress = await getBatchProgress(redis, progressKey, resume_from);
    
    console.log(`📊 Batch attribution progress:`, {
      conversions_processed: existingProgress.conversions_processed,
      attributions_created: existingProgress.attributions_created,
      resume_from: existingProgress.resume_from
    });
    
    // Step 1: Get conversions to process
    console.log('📋 Step 1: Getting conversions to process...');
    const conversionsToProcess = await getConversionsForBatchProcessing(redis, query_type, { date, emails, limit, resume_from: existingProgress.resume_from });
    
    if (conversionsToProcess.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          message: 'No conversions found to process',
          query_type: query_type,
          batch_complete: true
        })
      };
    }
    
    console.log(`📊 Found ${conversionsToProcess.length} conversions to process`);
    
    // Step 2: Process conversions in batch
    console.log('⚡ Step 2: Processing conversions in batch...');
    const batchResult = await processBatchAttributions(
      redis, 
      conversionsToProcess, 
      existingProgress,
      maxProcessingTime - (Date.now() - startTime)
    );
    
    // Step 3: Update progress
    const updatedProgress = {
      ...existingProgress,
      conversions_processed: existingProgress.conversions_processed + batchResult.processed_this_run,
      attributions_created: existingProgress.attributions_created + batchResult.attributions_created_this_run,
      attribution_failures: existingProgress.attribution_failures + batchResult.failures_this_run,
      resume_from: batchResult.final_resume_point,
      last_updated: new Date().toISOString(),
      is_complete: batchResult.batch_complete
    };
    
    await saveBatchProgress(redis, progressKey, updatedProgress);
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ Batch attribution completed in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        batch_attribution_summary: {
          // This run stats
          conversions_processed_this_run: batchResult.processed_this_run,
          attributions_created_this_run: batchResult.attributions_created_this_run,
          attribution_failures_this_run: batchResult.failures_this_run,
          processing_time_ms: totalTime,
          
          // Total progress
          total_conversions_processed: updatedProgress.conversions_processed,
          total_attributions_created: updatedProgress.attributions_created,
          total_attribution_failures: updatedProgress.attribution_failures,
          
          // Batch status
          batch_complete: batchResult.batch_complete,
          query_type: query_type,
          query_parameters: { date, emails: emails?.length || 0, limit }
        },
        
        attribution_samples: batchResult.attribution_samples, // Sample results for verification
        
        performance_stats: {
          attributions_per_second: Math.round(batchResult.attributions_created_this_run / (totalTime / 1000)),
          average_attribution_time_ms: batchResult.processed_this_run > 0 ? Math.round(totalTime / batchResult.processed_this_run) : 0,
          success_rate: batchResult.processed_this_run > 0 ? `${(((batchResult.processed_this_run - batchResult.failures_this_run) / batchResult.processed_this_run) * 100).toFixed(1)}%` : '0%'
        },
        
        next_steps: batchResult.batch_complete ? [
          '✅ Batch attribution processing complete!',
          'All requested conversions have been processed',
          'Attribution results stored permanently in Redis',
          'Use attribution analytics endpoints for reporting'
        ] : [
          'Batch processing in progress...',
          'Run the same command again to continue processing',
          `Progress: ${updatedProgress.conversions_processed} conversions processed`,
          `Resume point: ${batchResult.final_resume_point || 'continuing from next batch'}`
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Batch attribution failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Batch attribution failed', 
        message: error.message 
      })
    };
  }
};

// Get batch processing progress
async function getBatchProgress(redis, progressKey, resumeFrom) {
  try {
    const progressData = await redis(`get/${progressKey}`);
    if (progressData?.result) {
      const progress = JSON.parse(decodeURIComponent(progressData.result));
      console.log(`🔄 Found existing batch progress: ${progress.conversions_processed} conversions processed`);
      
      // Override resume point if explicitly provided
      if (resumeFrom) {
        progress.resume_from = resumeFrom;
        console.log(`🎯 Resume point overridden to: ${resumeFrom}`);
      }
      
      return progress;
    }
  } catch (error) {
    console.log('⚠️ No existing batch progress found, starting fresh');
  }
  
  return {
    conversions_processed: 0,
    attributions_created: 0,
    attribution_failures: 0,
    resume_from: resumeFrom || null,
    started_at: new Date().toISOString(),
    is_complete: false
  };
}

// Save batch processing progress
async function saveBatchProgress(redis, progressKey, progress) {
  await redis(`setex/${progressKey}/7200/${encodeURIComponent(JSON.stringify(progress))}`); // 2 hour TTL
  console.log(`💾 Batch progress saved: ${progress.conversions_processed} conversions, ${progress.attributions_created} attributions`);
}

// Get conversions for batch processing based on query type
async function getConversionsForBatchProcessing(redis, queryType, params) {
  const { date, emails, limit, resume_from } = params;
  const conversions = [];
  
  if (queryType === 'date') {
    if (!date) {
      throw new Error('Date parameter required for date query type');
    }
    
    console.log(`📅 Getting conversions for date: ${date}`);
    const dateIndexKey = `conversion_index_v1_date:${date}`;
    const dateResult = await redis(`get/${dateIndexKey}`, 3000);
    
    if (dateResult?.result) {
      const dateIndex = JSON.parse(decodeURIComponent(dateResult.result));
      console.log(`📊 Found ${dateIndex.conversion_count} conversions for ${date}`);
      
      // Apply resume logic for date-based processing
      let startIndex = 0;
      if (resume_from) {
        const resumeIndex = dateIndex.conversions.findIndex(c => c.timestamp === resume_from);
        if (resumeIndex >= 0) {
          startIndex = resumeIndex + 1; // Start after the resume point
          console.log(`🎯 Resuming from index ${startIndex} (after ${resume_from})`);
        }
      }
      
      const selectedConversions = dateIndex.conversions.slice(startIndex, startIndex + limit);
      conversions.push(...selectedConversions.map(conv => ({
        ...conv,
        query_source: 'date_index',
        source_date: date
      })));
    }
    
  } else if (queryType === 'emails') {
    if (!emails || !Array.isArray(emails)) {
      throw new Error('Emails array required for emails query type');
    }
    
    console.log(`📧 Getting conversions for ${emails.length} emails`);
    let emailsToProcess = [...emails];
    
    // Apply resume logic for email-based processing
    if (resume_from) {
      const resumeIndex = emails.indexOf(resume_from);
      if (resumeIndex >= 0) {
        emailsToProcess = emails.slice(resumeIndex + 1); // Start after resume point
        console.log(`🎯 Resuming from email after: ${resume_from}`);
      }
    }
    
    // Process limited number of emails
    const limitedEmails = emailsToProcess.slice(0, limit);
    
    for (const email of limitedEmails) {
      try {
        const emailIndexKey = `conversion_index_v1_email:${encodeURIComponent(email)}`;
        const emailResult = await redis(`get/${emailIndexKey}`, 2000);
        
        if (emailResult?.result) {
          const emailIndex = JSON.parse(decodeURIComponent(emailResult.result));
          // Take most recent conversion for each email
          if (emailIndex.conversions.length > 0) {
            conversions.push({
              ...emailIndex.conversions[0], // Most recent
              query_source: 'email_index',
              source_email: email
            });
          }
        }
      } catch (emailError) {
        console.log(`⚠️ Error processing email ${email}: ${emailError.message}`);
      }
    }
    
  } else if (queryType === 'all') {
    console.log(`🌐 Scanning for all conversions (limit: ${limit})`);
    
    // Scan conversion indexes to find all emails
    let cursor = resume_from || '0';
    let scannedKeys = 0;
    
    do {
      const scanResult = await redis(`scan/${cursor}/match/conversion_index_v1_email:*/count/100`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      scannedKeys += keys.length;
      
      // Process a subset of keys to stay within time limits
      const keysToProcess = keys.slice(0, Math.min(limit - conversions.length, 20));
      
      for (const key of keysToProcess) {
        try {
          const emailResult = await redis(`get/${key}`, 1500);
          if (emailResult?.result) {
            const emailIndex = JSON.parse(decodeURIComponent(emailResult.result));
            if (emailIndex.conversions.length > 0) {
              conversions.push({
                ...emailIndex.conversions[0], // Most recent conversion per email
                query_source: 'scan_all',
                source_key: key
              });
            }
          }
        } catch (keyError) {
          console.log(`⚠️ Error processing key ${key}: ${keyError.message}`);
        }
        
        if (conversions.length >= limit) break;
      }
      
      if (conversions.length >= limit) break;
      
    } while (cursor !== '0');
    
    console.log(`📊 Scanned ${scannedKeys} email indexes, found ${conversions.length} conversions`);
  }
  
  console.log(`📋 Collected ${conversions.length} conversions for batch processing`);
  return conversions;
}

// Process multiple conversions in batch
async function processBatchAttributions(redis, conversions, existingProgress, maxTime) {
  const batchStartTime = Date.now();
  let processedThisRun = 0;
  let attributionsCreatedThisRun = 0;
  let failuresThisRun = 0;
  let finalResumePoint = null;
  const attributionSamples = [];
  
  console.log(`⚡ Processing ${conversions.length} conversions in batch...`);
  
  for (let i = 0; i < conversions.length; i++) {
    // Time check
    if (Date.now() - batchStartTime > maxTime - 3000) {
      console.log(`⏰ Time limit approaching, stopping at conversion ${i}/${conversions.length}`);
      break;
    }
    
    const conversion = conversions[i];
    finalResumePoint = conversion.timestamp; // Track resume point
    
    try {
      console.log(`🔄 Processing conversion ${i + 1}/${conversions.length}: ${conversion.email}`);
      
      // Check if attribution already exists
      const existingKey = `multi_touch_attribution:${conversion.email}:${conversion.timestamp}`;
      const existingResult = await redis(`get/${existingKey}`, 1000);
      
      if (existingResult?.result) {
        console.log(`⚠️ Attribution already exists, skipping: ${conversion.email}`);
        processedThisRun++;
        continue;
      }
      
      // Perform multi-touch attribution (reusing Phase 1 logic)
      const attributionResult = await performMultiTouchAttribution(redis, conversion);
      
      // Store attribution result permanently
      const storageResult = await storeAttributionResult(redis, attributionResult);
      
      if (storageResult.success) {
        attributionsCreatedThisRun++;
        
        // Collect sample for response (first 3 results)
        if (attributionSamples.length < 3) {
          attributionSamples.push({
            email: conversion.email,
            timestamp: conversion.timestamp,
            touchpoints: attributionResult.attribution_summary.total_touchpoints,
            journey_duration_days: attributionResult.attribution_summary.journey_duration_days,
            attribution_confidence: attributionResult.attribution_summary.attribution_confidence.score,
            first_touch_source: attributionResult.attribution_summary.first_touch?.source,
            last_touch_source: attributionResult.attribution_summary.last_touch?.source
          });
        }
        
        console.log(`✅ Attribution created: ${conversion.email} (${attributionResult.attribution_summary.total_touchpoints} touchpoints)`);
      } else {
        failuresThisRun++;
        console.log(`❌ Attribution storage failed: ${conversion.email}`);
      }
      
      processedThisRun++;
      
      // Progress logging every 10 conversions
      if (processedThisRun % 10 === 0) {
        console.log(`📊 Batch progress: ${processedThisRun}/${conversions.length} processed, ${attributionsCreatedThisRun} attributions created`);
      }
      
    } catch (conversionError) {
      failuresThisRun++;
      console.log(`❌ Error processing conversion ${conversion.email}: ${conversionError.message}`);
    }
  }
  
  const batchComplete = processedThisRun >= conversions.length;
  const batchTime = Date.now() - batchStartTime;
  
  console.log(`🏁 Batch processing summary:`);
  console.log(`   📊 Processed: ${processedThisRun}/${conversions.length}`);
  console.log(`   ✅ Attributions created: ${attributionsCreatedThisRun}`);
  console.log(`   ❌ Failures: ${failuresThisRun}`);
  console.log(`   ⏱️ Batch time: ${batchTime}ms`);
  console.log(`   🏁 Complete: ${batchComplete}`);
  
  return {
    processed_this_run: processedThisRun,
    attributions_created_this_run: attributionsCreatedThisRun,
    failures_this_run: failuresThisRun,
    batch_complete: batchComplete,
    final_resume_point: finalResumePoint,
    attribution_samples: attributionSamples,
    processing_time_ms: batchTime
  };
}

// Multi-touch attribution logic (reused from Phase 1)
async function performMultiTouchAttribution(redis, conversionData) {
  const conversionTime = new Date(conversionData.timestamp);
  const allPageviews = [];
  const seenPageviews = new Set();
  const attributionMethods = [];
  
  // Attribution Query 1: Session ID lookup
  if (conversionData.ssid) {
    try {
      const sessionKey = `attribution_index_v1_session:${conversionData.ssid}`;
      const sessionResult = await redis(`get/${sessionKey}`, 2000);
      
      if (sessionResult?.result) {
        const sessionIndex = JSON.parse(decodeURIComponent(sessionResult.result));
        attributionMethods.push('session_match');
        addPageviewsToJourney(sessionIndex.pageviews, 'session_match', allPageviews, seenPageviews, conversionTime);
      }
    } catch (sessionError) {
      // Continue with other methods
    }
  }
  
  // Attribution Query 2: Primary IP lookup
  if (conversionData.primary_ip) {
    try {
      const encodedPrimaryIP = encodeIPForKey(conversionData.primary_ip);
      const primaryIPKey = `attribution_index_v1_ip:${encodedPrimaryIP}`;
      const primaryIPResult = await redis(`get/${primaryIPKey}`, 2000);
      
      if (primaryIPResult?.result) {
        const ipIndex = JSON.parse(decodeURIComponent(primaryIPResult.result));
        if (!attributionMethods.includes('primary_ip_match')) {
          attributionMethods.push('primary_ip_match');
        }
        addPageviewsToJourney(ipIndex.pageviews, 'primary_ip_match', allPageviews, seenPageviews, conversionTime);
      }
    } catch (ipError) {
      // Continue with other methods
    }
  }
  
  // Attribution Query 3: Conversion IP lookup
  if (conversionData.conversion_ip && conversionData.conversion_ip !== conversionData.primary_ip) {
    try {
      const encodedConversionIP = encodeIPForKey(conversionData.conversion_ip);
      const conversionIPKey = `attribution_index_v1_ip:${encodedConversionIP}`;
      const conversionIPResult = await redis(`get/${conversionIPKey}`, 2000);
      
      if (conversionIPResult?.result) {
        const ipIndex = JSON.parse(decodeURIComponent(conversionIPResult.result));
        if (!attributionMethods.includes('conversion_ip_match')) {
          attributionMethods.push('conversion_ip_match');
        }
        addPageviewsToJourney(ipIndex.pageviews, 'conversion_ip_match', allPageviews, seenPageviews, conversionTime);
      }
    } catch (ipError) {
      // Continue processing
    }
  }
  
  // Sort and process customer journey
  const customerJourney = allPageviews.sort((a, b) => 
    new Date(a.timestamp) - new Date(b.timestamp)
  );
  
  customerJourney.forEach((pageview, index) => {
    pageview.touchpoint_number = index + 1;
  });
  
  const attributionSummary = calculateAttributionSummary(customerJourney, conversionData, attributionMethods);
  
  return {
    conversion: {
      email: conversionData.email,
      timestamp: conversionData.timestamp,
      order_total: conversionData.order_total,
      landing_page: conversionData.landing_page,
      conversion_ip: conversionData.conversion_ip,
      primary_ip: conversionData.primary_ip,
      ssid: conversionData.ssid
    },
    attribution_summary: attributionSummary,
    customer_journey: customerJourney
  };
}

// Helper functions (reused from Phase 1)
function addPageviewsToJourney(pageviews, attributionMethod, allPageviews, seenPageviews, conversionTime) {
  pageviews.forEach(pageview => {
    if (new Date(pageview.timestamp) >= conversionTime) return;
    
    const dedupeKey = `${pageview.session_id}_${pageview.timestamp}`;
    if (!seenPageviews.has(dedupeKey)) {
      seenPageviews.add(dedupeKey);
      allPageviews.push({
        ...pageview,
        attribution_method: attributionMethod
      });
    }
  });
}

function calculateAttributionSummary(customerJourney, conversionData, attributionMethods) {
  if (customerJourney.length === 0) {
    return {
      total_touchpoints: 0,
      journey_duration_days: 0,
      attribution_methods_used: attributionMethods,
      first_touch: null,
      last_touch: null,
      attribution_confidence: { score: 0, factors: {} }
    };
  }
  
  const firstTouch = customerJourney[0];
  const lastTouch = customerJourney[customerJourney.length - 1];
  
  const journeyDurationMs = new Date(lastTouch.timestamp) - new Date(firstTouch.timestamp);
  const journeyDurationDays = Math.ceil(journeyDurationMs / (1000 * 60 * 60 * 24));
  
  return {
    total_touchpoints: customerJourney.length,
    journey_duration_days: journeyDurationDays,
    attribution_methods_used: attributionMethods,
    first_touch: {
      timestamp: firstTouch.timestamp,
      landing_page: firstTouch.landing_page,
      source: firstTouch.source,
      utm_campaign: firstTouch.utm_campaign,
      attribution_method: firstTouch.attribution_method
    },
    last_touch: {
      timestamp: lastTouch.timestamp,
      landing_page: lastTouch.landing_page,
      source: lastTouch.source,
      utm_campaign: lastTouch.utm_campaign,
      attribution_method: lastTouch.attribution_method
    },
    attribution_confidence: { score: Math.min(50 + (customerJourney.length * 5), 100), factors: {} }
  };
}

async function storeAttributionResult(redis, attributionResult) {
  const storageKey = `multi_touch_attribution:${attributionResult.conversion.email}:${attributionResult.conversion.timestamp}`;
  
  const enrichedResult = {
    ...attributionResult,
    storage_metadata: {
      stored_at: new Date().toISOString(),
      storage_key: storageKey,
      attribution_version: 'v1',
      storage_permanent: true
    }
  };
  
  try {
    await redis(`set/${storageKey}/${encodeURIComponent(JSON.stringify(enrichedResult))}`);
    return { success: true, key: storageKey, verified: true };
  } catch (error) {
    return { success: false, error: error.message, verified: false };
  }
}

function encodeIPForKey(ip) {
  return ip.replace(/:/g, '_').replace(/\./g, '_');
}

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
