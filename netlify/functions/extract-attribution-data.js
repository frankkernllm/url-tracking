// Multi-Source Attribution Data Extractor
// Path: netlify/functions/extract-attribution-data.js
// Purpose: Extract ALL pageviews from multiple sources into attribution-optimized chunks

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
    const redis = initializeRedis();
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max
    
    console.log('🚀 Starting MULTI-SOURCE attribution data extraction...');
    
    // Load existing progress or start fresh
    const progressKey = 'attribution_extraction_v1_progress';
    const existingProgress = await getAttributionProgress(redis, progressKey);
    
    console.log(`📊 Resuming attribution extraction:`, {
      total_extracted: existingProgress.total_extracted,
      current_pattern_index: existingProgress.current_pattern_index,
      current_pattern: existingProgress.patterns[existingProgress.current_pattern_index],
      last_cursor: existingProgress.last_cursor,
      chunks_completed: existingProgress.chunks_completed
    });
    
    // MULTI-SOURCE extraction with smart resume
    const extractionResult = await extractMultiSourceAttributionData(
      redis, 
      existingProgress,
      maxProcessingTime - (Date.now() - startTime)
    );
    
    // Update progress after extraction
    const updatedProgress = {
      ...existingProgress,
      total_extracted: existingProgress.total_extracted + extractionResult.pageviews_extracted_this_run,
      total_keys_scanned: existingProgress.total_keys_scanned + extractionResult.keys_scanned_this_run,
      current_pattern_index: extractionResult.final_pattern_index,
      last_cursor: extractionResult.final_cursor,
      chunks_completed: existingProgress.chunks_completed + extractionResult.chunks_processed_this_run,
      chunks_stored: existingProgress.chunks_stored + extractionResult.chunks_stored_this_run,
      unique_ips_found: extractionResult.total_unique_ips,
      unique_sessions_found: extractionResult.total_unique_sessions,
      last_updated: new Date().toISOString(),
      is_complete: extractionResult.is_complete,
      earliest_pageview: extractionResult.earliest_pageview || existingProgress.earliest_pageview,
      latest_pageview: extractionResult.latest_pageview || existingProgress.latest_pageview,
      patterns_completed: extractionResult.patterns_completed
    };
    
    await storeAttributionProgress(redis, progressKey, updatedProgress);
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ MULTI-SOURCE attribution extraction finished in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        extraction_complete: extractionResult.is_complete,
        attribution_extraction_summary: {
          // This run stats
          pageviews_extracted_this_run: extractionResult.pageviews_extracted_this_run,
          keys_scanned_this_run: extractionResult.keys_scanned_this_run,
          chunks_processed_this_run: extractionResult.chunks_processed_this_run,
          processing_time_ms: totalTime,
          
          // Total stats across all runs
          total_pageviews_extracted: updatedProgress.total_extracted,
          total_keys_scanned: updatedProgress.total_keys_scanned,
          total_chunks_completed: updatedProgress.chunks_completed,
          extraction_method: 'multi_source_attribution_v1'
        },
        multi_source_progress: {
          patterns_available: existingProgress.patterns.length,
          patterns_completed: extractionResult.patterns_completed.length,
          current_pattern: updatedProgress.current_pattern_index < existingProgress.patterns.length ? 
            existingProgress.patterns[updatedProgress.current_pattern_index] : 'ALL_COMPLETE',
          patterns_remaining: existingProgress.patterns.length - extractionResult.patterns_completed.length
        },
        performance: {
          pageviews_per_second_this_run: Math.round(extractionResult.pageviews_extracted_this_run / (totalTime / 1000)),
          unique_ips_found: updatedProgress.unique_ips_found,
          unique_sessions_found: updatedProgress.unique_sessions_found
        },
        coverage: {
          earliest_pageview: updatedProgress.earliest_pageview,
          latest_pageview: updatedProgress.latest_pageview,
          attribution_fields_coverage: extractionResult.attribution_fields_found
        },
        next_steps: extractionResult.is_complete ? [
          '✅ Multi-source attribution extraction complete!',
          'All pageview patterns processed successfully',
          'Run build-attribution-indexes.js to build attribution indexes',
          'System ready for attribution analysis'
        ] : [
          'Multi-source extraction continuing...',
          'Run the same command again to continue processing',
          `Currently processing: ${existingProgress.patterns[updatedProgress.current_pattern_index] || 'COMPLETING'}`,
          `Progress: ${extractionResult.patterns_completed.length}/${existingProgress.patterns.length} patterns complete`
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Multi-source attribution extraction failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Multi-source attribution extraction failed', 
        message: error.message 
      })
    };
  }
};

// Get attribution extraction progress (multi-source aware)
async function getAttributionProgress(redis, progressKey) {
  try {
    const progressData = await redis(`get/${progressKey}`);
    
    if (progressData?.result) {
      const progress = JSON.parse(decodeURIComponent(progressData.result));
      console.log(`🔄 Found existing attribution progress: ${progress.total_extracted} pageviews extracted`);
      return progress;
    }
  } catch (error) {
    console.log('⚠️ No existing attribution progress found, starting fresh');
  }
  
  // Default fresh start with all 3 patterns
  return {
    total_extracted: 0,
    total_keys_scanned: 0,
    patterns: [
      'attribution_*',        // Pattern 1: Main source (IPv4 & IPv6)
      'pageviews:*',         // Pattern 2: Newer format (July 2025 data)
      'attribution:*'        // Pattern 3: Legacy colon format (June 2025 data)
    ],
    current_pattern_index: 0,
    last_cursor: '0',
    chunks_completed: 0,
    chunks_stored: 0,
    unique_ips_found: 0,
    unique_sessions_found: 0,
    started_at: new Date().toISOString(),
    is_complete: false,
    earliest_pageview: null,
    latest_pageview: null,
    patterns_completed: []
  };
}

// Store attribution progress
async function storeAttributionProgress(redis, progressKey, progress) {
  await redis(`setex/${progressKey}/3600/${encodeURIComponent(JSON.stringify(progress))}`); // 1 hour TTL
  console.log(`💾 Attribution progress saved: ${progress.total_extracted} total pageviews, pattern ${progress.current_pattern_index}/${progress.patterns.length}`);
}

// Extract attribution data from multiple sources with smart resume
async function extractMultiSourceAttributionData(redis, existingProgress, maxTime) {
  const extractionStartTime = Date.now();
  let thisRunPageviews = [];
  let thisRunKeysScanned = 0;
  let thisRunChunksProcessed = 0;
  let thisRunChunksStored = 0;
  let allUniqueIPs = new Set();
  let allUniqueSessions = new Set();
  let earliestPageview = existingProgress.earliest_pageview;
  let latestPageview = existingProgress.latest_pageview;
  let attributionFieldsFound = new Set();
  
  let currentPatternIndex = existingProgress.current_pattern_index;
  let currentCursor = existingProgress.last_cursor;
  let patternsCompleted = [...existingProgress.patterns_completed];
  
  console.log(`🔄 MULTI-SOURCE RESUME: Starting from pattern ${currentPatternIndex}, cursor: ${currentCursor}`);
  console.log(`📊 Previous progress: ${existingProgress.total_extracted} pageviews from ${patternsCompleted.length} patterns`);
  
  try {
    // Process remaining patterns
    while (currentPatternIndex < existingProgress.patterns.length) {
      const currentPattern = existingProgress.patterns[currentPatternIndex];
      console.log(`🎯 Processing pattern ${currentPatternIndex + 1}/${existingProgress.patterns.length}: ${currentPattern}`);
      
      // Check time remaining for this pattern
      const timeRemaining = maxTime - (Date.now() - extractionStartTime);
      if (timeRemaining < 5000) {
        console.log(`⏰ Time limit approaching: ${timeRemaining}ms remaining, stopping pattern processing`);
        break;
      }
      
      // Process current pattern
      const patternResult = await processAttributionPattern(
        redis, 
        currentPattern, 
        currentCursor, 
        timeRemaining - 2000
      );
      
      // Accumulate results
      thisRunPageviews.push(...patternResult.pageviews);
      thisRunKeysScanned += patternResult.keys_scanned;
      thisRunChunksProcessed += patternResult.chunks_processed;
      
      // Track unique values
      patternResult.unique_ips.forEach(ip => allUniqueIPs.add(ip));
      patternResult.unique_sessions.forEach(session => allUniqueSessions.add(session));
      patternResult.attribution_fields.forEach(field => attributionFieldsFound.add(field));
      
      // Update time range
      if (patternResult.earliest_pageview && (!earliestPageview || new Date(patternResult.earliest_pageview) < new Date(earliestPageview))) {
        earliestPageview = patternResult.earliest_pageview;
      }
      if (patternResult.latest_pageview && (!latestPageview || new Date(patternResult.latest_pageview) > new Date(latestPageview))) {
        latestPageview = patternResult.latest_pageview;
      }
      
      // Check if pattern is complete
      if (patternResult.pattern_complete) {
        console.log(`✅ Pattern ${currentPattern} completed successfully`);
        patternsCompleted.push(currentPattern);
        currentPatternIndex++;
        currentCursor = '0'; // Reset cursor for next pattern
      } else {
        console.log(`⏸️ Pattern ${currentPattern} incomplete, will resume from cursor: ${patternResult.final_cursor}`);
        currentCursor = patternResult.final_cursor;
        break; // Time limit reached, will resume next run
      }
    }
    
    // Store accumulated pageviews in attribution chunks
    if (thisRunPageviews.length > 0) {
      const chunkResult = await storeAttributionChunks(redis, thisRunPageviews, existingProgress.chunks_stored);
      thisRunChunksStored = chunkResult.chunks_stored;
      console.log(`💾 Stored ${thisRunChunksStored} attribution chunks with ${thisRunPageviews.length} total pageviews`);
    }
    
    const isComplete = currentPatternIndex >= existingProgress.patterns.length;
    const processingTime = Date.now() - extractionStartTime;
    
    console.log(`🏁 MULTI-SOURCE attribution extraction summary:`);
    console.log(`   📊 This run pageviews: ${thisRunPageviews.length}`);
    console.log(`   📊 Total pageviews: ${existingProgress.total_extracted + thisRunPageviews.length}`);
    console.log(`   🔍 This run keys scanned: ${thisRunKeysScanned}`);
    console.log(`   📦 This run chunks: ${thisRunChunksProcessed}`);
    console.log(`   💾 This run chunks stored: ${thisRunChunksStored}`);
    console.log(`   🌐 Unique IPs found: ${allUniqueIPs.size}`);
    console.log(`   🔗 Unique sessions found: ${allUniqueSessions.size}`);
    console.log(`   🎯 Patterns completed: ${patternsCompleted.length}/${existingProgress.patterns.length}`);
    console.log(`   ✅ Complete: ${isComplete}`);
    console.log(`   ⏱️ This run time: ${processingTime}ms`);
    
    return {
      pageviews_extracted_this_run: thisRunPageviews.length,
      keys_scanned_this_run: thisRunKeysScanned,
      chunks_processed_this_run: thisRunChunksProcessed,
      chunks_stored_this_run: thisRunChunksStored,
      total_unique_ips: allUniqueIPs.size,
      total_unique_sessions: allUniqueSessions.size,
      earliest_pageview: earliestPageview,
      latest_pageview: latestPageview,
      attribution_fields_found: Array.from(attributionFieldsFound),
      is_complete: isComplete,
      final_pattern_index: currentPatternIndex,
      final_cursor: currentCursor,
      patterns_completed: patternsCompleted,
      processing_time_ms: processingTime
    };
    
  } catch (error) {
    console.error('❌ Multi-source attribution extraction error:', error);
    return {
      pageviews_extracted_this_run: thisRunPageviews.length,
      keys_scanned_this_run: thisRunKeysScanned,
      chunks_processed_this_run: thisRunChunksProcessed,
      chunks_stored_this_run: thisRunChunksStored,
      total_unique_ips: allUniqueIPs.size,
      total_unique_sessions: allUniqueSessions.size,
      is_complete: false,
      final_pattern_index: currentPatternIndex,
      final_cursor: currentCursor,
      patterns_completed: patternsCompleted,
      error: error.message
    };
  }
}

// Process individual attribution pattern
async function processAttributionPattern(redis, pattern, startCursor, maxTime) {
  const patternStartTime = Date.now();
  let cursor = startCursor;
  let pageviews = [];
  let keysScanned = 0;
  let chunksProcessed = 0;
  let uniqueIPs = new Set();
  let uniqueSessions = new Set();
  let attributionFields = new Set();
  let earliestPageview = null;
  let latestPageview = null;
  
  console.log(`🎯 Processing attribution pattern: ${pattern} from cursor: ${cursor}`);
  
  try {
    do {
      // Check time remaining
      const timeRemaining = maxTime - (Date.now() - patternStartTime);
      if (timeRemaining < 3000) {
        console.log(`⏰ Pattern time limit approaching: ${timeRemaining}ms remaining`);
        break;
      }
      
      console.log(`🔍 Pattern scan cursor: ${cursor}, collected: ${pageviews.length} pageviews`);
      
      // Scan for next batch
      const scanResult = await redis(`scan/${cursor}/match/${pattern}/count/500`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        console.log(`🏁 Pattern scan complete: no more results for ${pattern}`);
        cursor = '0'; // Mark pattern as complete
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      keysScanned += keys.length;
      chunksProcessed++;
      
      console.log(`📊 Pattern chunk ${chunksProcessed}: Found ${keys.length} keys, cursor: ${cursor}`);
      
      // Filter and extract attribution-relevant keys
      const attributionKeys = filterAttributionKeys(keys, pattern);
      console.log(`📝 Filtered: ${attributionKeys.length} attribution keys from ${keys.length} total`);
      
      if (attributionKeys.length === 0) {
        console.log(`⚠️ No attribution keys in this chunk, continuing...`);
        continue;
      }
      
      // Process keys in batches
      const batchSize = 50;
      for (let i = 0; i < attributionKeys.length; i += batchSize) {
        const timeCheck = maxTime - (Date.now() - patternStartTime);
        if (timeCheck < 2000) {
          console.log(`⏰ Time limit during batch processing`);
          break;
        }
        
        const batch = attributionKeys.slice(i, i + batchSize);
        const batchPageviews = await processAttributionKeyBatch(redis, batch, pattern);
        
        // Extract attribution data
        batchPageviews.forEach(pv => {
          if (pv && pv.timestamp && pv.ip_address) {
            // Track attribution fields
            Object.keys(pv).forEach(field => attributionFields.add(field));
            
            // Track unique values
            uniqueIPs.add(pv.ip_address);
            if (pv.session_id) uniqueSessions.add(pv.session_id);
            
            // Track time range
            const pvTime = new Date(pv.timestamp);
            if (!earliestPageview || pvTime < new Date(earliestPageview)) {
              earliestPageview = pv.timestamp;
            }
            if (!latestPageview || pvTime > new Date(latestPageview)) {
              latestPageview = pv.timestamp;
            }
            
            pageviews.push(pv);
          }
        });
        
        console.log(`📦 Pattern batch processed: ${batchPageviews.length} pageviews`);
      }
      
      // Safety check: don't run forever
      if (chunksProcessed >= 50) {
        console.log(`🛑 Pattern safety limit: processed 50 chunks, stopping`);
        break;
      }
      
    } while (cursor !== '0' && Date.now() - patternStartTime < maxTime - 2000);
    
    const patternComplete = cursor === '0';
    const patternTime = Date.now() - patternStartTime;
    
    console.log(`🎯 Pattern ${pattern} summary:`);
    console.log(`   📊 Pageviews extracted: ${pageviews.length}`);
    console.log(`   🔍 Keys scanned: ${keysScanned}`);
    console.log(`   📦 Chunks processed: ${chunksProcessed}`);
    console.log(`   🌐 Unique IPs: ${uniqueIPs.size}`);
    console.log(`   🔗 Unique sessions: ${uniqueSessions.size}`);
    console.log(`   ✅ Complete: ${patternComplete}`);
    console.log(`   ⏱️ Pattern time: ${patternTime}ms`);
    
    return {
      pageviews: pageviews,
      keys_scanned: keysScanned,
      chunks_processed: chunksProcessed,
      unique_ips: Array.from(uniqueIPs),
      unique_sessions: Array.from(uniqueSessions),
      attribution_fields: Array.from(attributionFields),
      earliest_pageview: earliestPageview,
      latest_pageview: latestPageview,
      pattern_complete: patternComplete,
      final_cursor: cursor,
      processing_time_ms: patternTime
    };
    
  } catch (error) {
    console.error(`❌ Pattern ${pattern} processing error:`, error);
    return {
      pageviews: pageviews,
      keys_scanned: keysScanned,
      chunks_processed: chunksProcessed,
      unique_ips: Array.from(uniqueIPs),
      unique_sessions: Array.from(uniqueSessions),
      attribution_fields: Array.from(attributionFields),
      pattern_complete: false,
      final_cursor: cursor,
      error: error.message
    };
  }
}

// Filter keys for attribution relevance
function filterAttributionKeys(keys, pattern) {
  if (pattern === 'attribution_*') {
    // Filter main attribution keys - exclude auxiliary indexes
    return keys.filter(key => {
      return !key.includes('_ip_') && 
             !key.includes('_session_') && 
             !key.includes('_fp_') && 
             !key.includes('_screen_') && 
             !key.includes('_webgl_') && 
             !key.includes('_geo_') &&
             !key.includes('pageview_index_') &&
             !key.includes('conversion_index_') &&
             !key.includes('attribution_stats_') &&
             !key.includes('geo_cache:') &&
             !key.includes('_region_') &&
             !key.includes('_hw_') &&
             key.startsWith('attribution_') &&
             key.match(/\d+$/); // Must end with timestamp
    });
  } else if (pattern === 'pageviews:*') {
    // Newer format pageviews
    return keys.filter(key => key.startsWith('pageviews:') && !key.includes('index_'));
  } else if (pattern === 'attribution:*') {
    // Legacy colon format
    return keys.filter(key => key.startsWith('attribution:') && !key.includes('stats'));
  }
  
  return keys; // Default: return all keys
}

// Process batch of attribution keys
async function processAttributionKeyBatch(redis, keys, pattern) {
  const batchPromises = keys.map(async (key) => {
    try {
      const result = await redis(`get/${key}`, 1000);
      if (result?.result) {
        let parsed;
        try {
          parsed = JSON.parse(result.result);
        } catch (parseError) {
          try {
            parsed = JSON.parse(decodeURIComponent(result.result));
          } catch (decodeError) {
            return null;
          }
        }
        
        if (parsed && parsed.timestamp && parsed.ip_address) {
          // Standardize attribution data structure
          return {
            // Core attribution fields
            session_id: parsed.session_id || null,
            timestamp: parsed.timestamp,
            landing_page: parsed.landing_page || 'unknown',
            source: parsed.source || 'direct',
            ip_address: parsed.ip_address,
            
            // Canvas/WebGL fingerprints for matching
            canvas_fingerprint: parsed.canvas_fingerprint || null,
            webgl_fingerprint: parsed.webgl_fingerprint || null,
            
            // Attribution context
            referrer_url: parsed.referrer_url || null,
            utm_campaign: parsed.utm_campaign || null,
            utm_source: parsed.utm_source || null,
            utm_medium: parsed.utm_medium || null,
            utm_term: parsed.utm_term || null,
            utm_content: parsed.utm_content || null,
            
            // Technical data for matching
            screen_resolution: parsed.screen_resolution || null,
            cpu_cores: parsed.cpu_cores || null,
            memory_gb: parsed.memory_gb || null,
            
            // Original key reference
            redis_key: key,
            source_pattern: pattern
          };
        }
      }
    } catch (error) {
      // Skip errors to keep processing
    }
    return null;
  });
  
  const results = await Promise.all(batchPromises);
  return results.filter(result => result !== null);
}

// Store attribution data in optimized chunks
async function storeAttributionChunks(redis, pageviews, startingChunkNumber) {
  if (pageviews.length === 0) return { chunks_stored: 0 };
  
  const chunkSize = 1000; // 1000 pageviews per chunk
  let chunksStored = 0;
  
  for (let i = 0; i < pageviews.length; i += chunkSize) {
    const chunk = pageviews.slice(i, i + chunkSize);
    const chunkNumber = startingChunkNumber + chunksStored + 1;
    const chunkKey = `attribution_data_chunk:v1_${chunkNumber}:${Date.now()}`;
    
    const chunkData = {
      chunk_id: `v1_${chunkNumber}`,
      pageview_count: chunk.length,
      pageviews: chunk,
      created_at: new Date().toISOString(),
      version: 'v1',
      attribution_ready: true
    };
    
    try {
      await redis(`setex/${chunkKey}/2592000/${encodeURIComponent(JSON.stringify(chunkData))}`); // 30 days TTL
      chunksStored++;
      console.log(`💾 Stored attribution chunk ${chunkNumber} with ${chunk.length} pageviews`);
    } catch (error) {
      console.error(`❌ Failed to store chunk ${chunkNumber}:`, error);
    }
  }
  
  return { chunks_stored: chunksStored };
}

// Initialize Redis helper
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
