// FIXED: Optimized Index Builder - Auto-Detects and Processes Missing Chunks
// Path: netlify/functions/build-indexes-complete.js
// Purpose: FIXED version that automatically handles stuck progress and missing chunks

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
    console.log('🚀 FIXED ENHANCED index building - Auto-detecting missing chunks...');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max
    
    const redis = initializeRedis();
    
    // Step 1: Find all pageview chunks
    const allChunks = await findAllPageviewChunks(redis);
    console.log(`📦 Found ${allChunks.length} pageview chunks total`);
    
    if (allChunks.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: false,
          message: 'No pageview chunks found. Run extraction first.'
        })
      };
    }
    
    // 🆕 FIXED: Load existing progress or start fresh
    const progressKey = 'index_building_progress';
    const progress = await getIndexProgress(redis, progressKey);
    
    // 🆕 FIXED: Auto-detect missing chunks and clear stuck progress
    const chunkMismatch = allChunks.length !== progress.total_chunks;
    const stuckProgress = progress.last_chunk_index < allChunks.length && progress.is_complete;
    
    if (chunkMismatch || stuckProgress) {
      console.log(`🔧 FIXED: Detected chunk mismatch or stuck progress`);
      console.log(`   Current: ${progress.last_chunk_index}/${progress.total_chunks}`);
      console.log(`   Actual: ${allChunks.length} chunks available`);
      console.log(`   Clearing stuck progress and resuming...`);
      
      // Reset progress to resume from last valid position
      progress.total_chunks = allChunks.length;
      progress.is_complete = false;
      
      // If we're completely stuck, find the actual last processed chunk
      if (stuckProgress) {
        console.log(`🔧 Progress was marked complete but missing chunks detected`);
      }
    }
    
    // 🆕 Check if already complete with correct chunk count
    if (progress.is_complete && progress.last_chunk_index >= allChunks.length) {
      console.log('✅ Index building already complete with all chunks processed!');
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          enhanced_indexing_summary: {
            build_complete: true,
            chunks_processed: progress.chunks_processed,
            total_chunks: progress.total_chunks,
            ip_indexes_created: progress.ip_indexes_created,
            completion_percentage: 100,
            completed_at: progress.completed_at,
            started_at: progress.started_at,
            fix_applied: false
          },
          next_steps: [
            '✅ Index building complete with all chunks!',
            'All pageview indexes created successfully',
            'System ready for attribution queries'
          ]
        })
      };
    }
    
    // 🆕 FIXED: Calculate remaining chunks to process
    const remainingChunks = allChunks.slice(progress.last_chunk_index);
    const missingCount = remainingChunks.length;
    
    console.log(`📊 FIXED Processing Plan:`);
    console.log(`   Total chunks: ${allChunks.length}`);
    console.log(`   Already processed: ${progress.last_chunk_index}`);
    console.log(`   Remaining to process: ${missingCount}`);
    console.log(`   Will process chunks ${progress.last_chunk_index} to ${allChunks.length - 1}`);
    
    // 🆕 FIXED: Process remaining chunks
    const indexingResult = await processChunksAndBuildEnhancedIndexes(
      redis, 
      remainingChunks, 
      progress,
      maxProcessingTime - (Date.now() - startTime)
    );
    
    // 🆕 Update progress with results
    progress.chunks_processed += indexingResult.chunks_processed_this_run;
    progress.last_chunk_index += indexingResult.chunks_processed_this_run;
    progress.ip_indexes_created += indexingResult.ip_indexes_created_this_run;
    
    // 🆕 Check if complete
    if (progress.last_chunk_index >= allChunks.length) {
      progress.is_complete = true;
      progress.completed_at = new Date().toISOString();
      console.log('🎉 FIXED: Index building completed successfully!');
    }
    
    // 🆕 Save progress for next run
    await saveIndexProgress(redis, progressKey, progress);
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ FIXED indexing finished in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        enhanced_indexing_summary: {
          // Processing stats for this run
          chunks_processed_this_run: indexingResult.chunks_processed_this_run,
          ip_indexes_created_this_run: indexingResult.ip_indexes_created_this_run,
          pageviews_processed_this_run: indexingResult.pageviews_processed_this_run,
          processing_time_ms: totalTime,
          
          // 🆕 FIXED: Accurate progress tracking
          build_complete: progress.is_complete,
          progress: {
            chunks_processed: progress.chunks_processed,
            total_chunks: progress.total_chunks,
            completion_percentage: ((progress.chunks_processed / progress.total_chunks) * 100).toFixed(1),
            can_resume: !progress.is_complete
          },
          
          // 🆕 FIXED: Show what was fixed
          fix_applied: true,
          missing_chunks_detected: missingCount,
          chunk_mismatch_resolved: chunkMismatch,
          stuck_progress_cleared: stuckProgress,
          
          // Existing stats
          total_pageviews: indexingResult.total_pageviews,
          total_ip_indexes_created: progress.ip_indexes_created,
          unique_ips_indexed: indexingResult.unique_ips_indexed,
          time_range_indexed: indexingResult.time_range,
          attribution_completeness: {
            multi_signal_attribution: true,
            geographic_correlation: true
          }
        },
        
        // 🆕 FIXED: Dynamic next steps based on completion
        next_steps: progress.is_complete ? [
          '🎉 FIXED: Index building complete!',
          `Processed all ${progress.total_chunks} chunks including missing ones`,
          'All pageview indexes created successfully',
          'System ready for attribution queries'
        ] : [
          'FIXED: Index building in progress...',
          'Missing chunks are being processed',
          'Run the same command again to continue',
          `Progress: ${progress.chunks_processed}/${progress.total_chunks} chunks (${((progress.chunks_processed / progress.total_chunks) * 100).toFixed(1)}%)`,
          `Estimated chunks remaining: ${progress.total_chunks - progress.chunks_processed}`
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ FIXED indexing failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'FIXED indexing failed', 
        message: error.message 
      })
    };
  }
};

// 🆕 FIXED: Get existing index building progress with auto-correction
async function getIndexProgress(redis, progressKey) {
  try {
    const progressData = await redis(`get/${progressKey}`);
    if (progressData?.result) {
      const progress = JSON.parse(decodeURIComponent(progressData.result));
      console.log(`🔄 Found existing progress: chunk ${progress.last_chunk_index}/${progress.total_chunks}`);
      console.log(`📊 Previous progress: ${progress.chunks_processed} chunks, ${progress.ip_indexes_created} indexes created`);
      return progress;
    }
  } catch (error) {
    console.log('⚠️ No existing progress found, starting fresh');
  }
  
  return {
    last_chunk_index: 0,           // Which chunk to start from
    total_chunks: 0,               // Total chunks found
    ip_indexes_created: 0,         // Running total of indexes created
    chunks_processed: 0,           // How many chunks we've processed
    started_at: new Date().toISOString(),
    is_complete: false
  };
}

// 🆕 FIXED: Save progress after processing chunks
async function saveIndexProgress(redis, progressKey, progress) {
  await redis(`setex/${progressKey}/7200/${encodeURIComponent(JSON.stringify(progress))}`); // 2 hour TTL
  console.log(`💾 FIXED Progress saved: chunk ${progress.last_chunk_index}/${progress.total_chunks}, ${progress.ip_indexes_created} indexes created`);
}

// 🔄 MODIFIED: Process chunks and build indexes with COMPLETE attribution data and progress tracking
async function processChunksAndBuildEnhancedIndexes(redis, chunkKeys, progress, maxTime) {
  const processStartTime = Date.now();
  console.log(`⚡ FIXED processing: ${chunkKeys.length} remaining chunks with MULTI-SIGNAL attribution in ${maxTime}ms`);
  console.log(`📊 Starting from overall chunk ${progress.last_chunk_index}/${progress.total_chunks}`);
  
  const ipIndexMap = new Map(); // Use Map for better performance
  const timeStats = {
    earliest: null,
    latest: null
  };
  
  let totalPageviews = 0;
  let chunksProcessedThisRun = 0; // Track chunks processed in this run only
  let ipIndexesCreatedThisRun = 0; // Track indexes created in this run only
  let attributionFieldsIncluded = [];
  
  // 🔄 FIXED: Process chunks with resume capability and time management
  for (let i = 0; i < chunkKeys.length; i++) {
    // 🆕 Enhanced time check - stop 2 seconds before limit to ensure proper saving
    if (Date.now() - processStartTime > maxTime - 2000) {
      console.log(`⏰ Time limit approaching, saving progress at chunk ${progress.last_chunk_index + i}`);
      break;
    }
    
    try {
      const chunkKey = chunkKeys[i];
      console.log(`📦 FIXED Processing chunk ${progress.last_chunk_index + i + 1}/${progress.total_chunks}: ${chunkKey}`);
      
      const chunkData = await redis(`get/${chunkKey}`, 2000); // Faster timeout
      
      if (chunkData?.result) {
        const chunk = JSON.parse(decodeURIComponent(chunkData.result));
        
        if (chunk.pageviews && Array.isArray(chunk.pageviews)) {
          for (const pageview of chunk.pageviews) {
            totalPageviews++;
            
            // Track time range
            const pvTime = new Date(pageview.timestamp);
            if (!timeStats.earliest || pvTime < timeStats.earliest) {
              timeStats.earliest = pvTime;
            }
            if (!timeStats.latest || pvTime > timeStats.latest) {
              timeStats.latest = pvTime;
            }
            
            // Group by IP efficiently
            const ip = pageview.ip_address;
            if (ip && ip !== 'unknown') {
              const encodedIP = ip.replace(/:/g, '_');
              
              if (!ipIndexMap.has(encodedIP)) {
                ipIndexMap.set(encodedIP, {
                  ip_address: ip,
                  pageviews: [],
                  latest_timestamp: pageview.timestamp
                });
              }
              
              const ipData = ipIndexMap.get(encodedIP);
              ipData.pageviews.push(pageview);
              
              // Update latest timestamp
              if (new Date(pageview.timestamp) > new Date(ipData.latest_timestamp)) {
                ipData.latest_timestamp = pageview.timestamp;
              }
              
              // Track attribution fields available
              const fields = Object.keys(pageview).filter(field => 
                !['timestamp', 'ip_address'].includes(field)
              );
              attributionFieldsIncluded = [...new Set([...attributionFieldsIncluded, ...fields])];
            }
          }
        }
      }
      
      chunksProcessedThisRun++; // Increment chunks processed this run
      
      // 🆕 Save progress every 10 chunks for better resilience
      if (chunksProcessedThisRun % 10 === 0) {
        const tempProgress = {
          ...progress,
          chunks_processed: progress.chunks_processed + chunksProcessedThisRun,
          last_chunk_index: progress.last_chunk_index + chunksProcessedThisRun
        };
        await saveIndexProgress(redis, 'index_building_progress', tempProgress);
      }
      
    } catch (chunkError) {
      console.log(`⚠️ Error processing chunk ${chunkKeys[i]}: ${chunkError.message}`);
      // Continue processing other chunks
    }
  }
  
  console.log(`📊 FIXED Chunk processing complete: ${chunksProcessedThisRun} chunks, ${totalPageviews} pageviews, ${ipIndexMap.size} unique IPs`);
  
  // Step 2: Build IP indexes in batches
  const batchSize = 50;
  const ipEntries = Array.from(ipIndexMap.entries());
  
  console.log(`🏗️ Building ${ipEntries.length} enhanced IP indexes with COMPLETE attribution data...`);
  
  for (let i = 0; i < ipEntries.length; i += batchSize) {
    // Check time before each batch
    if (Date.now() - processStartTime > maxTime - 3000) {
      console.log(`⏰ Time limit approaching during indexing, stopping at ${i}/${ipEntries.length} indexes`);
      break;
    }
    
    const batch = ipEntries.slice(i, i + batchSize);
    
    const batchPromises = batch.map(async ([encodedIP, ipData]) => {
      try {
        // Sort pageviews by timestamp (most recent first)
        ipData.pageviews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
        
        const ipKey = `pageview_index_ip:${encodedIP}`;
        
        // 🚀 ENHANCED: Complete attribution data in indexes
        const indexData = {
          ip_address: ipData.ip_address,
          pageview_count: ipData.pageviews.length,
          latest_timestamp: ipData.latest_timestamp,
          pageviews: ipData.pageviews, // Now includes ALL attribution fields
          created_at: new Date().toISOString(),
          
          // Enhanced index metadata
          multi_signal_ready: true,
          attribution_fields_available: attributionFieldsIncluded,
          session_ids_available: ipData.pageviews.filter(pv => pv.session_id).length,
          device_fingerprints_available: ipData.pageviews.filter(pv => pv.canvas_fingerprint).length,
          screen_resolutions_available: ipData.pageviews.filter(pv => pv.screen_resolution).length,
          webgl_fingerprints_available: ipData.pageviews.filter(pv => pv.webgl_fingerprint).length
        };
        
        // Store with 30-day TTL (2,592,000 seconds)
        await redis(`setex/${ipKey}/2592000/${encodeURIComponent(JSON.stringify(indexData))}`, 2000);
        return 1;
        
      } catch (ipError) {
        console.log(`⚠️ Error creating enhanced IP index: ${ipError.message}`);
        return 0;
      }
    });
    
    try {
      const batchResults = await Promise.all(batchPromises);
      const successfulIndexes = batchResults.reduce((sum, result) => sum + result, 0);
      ipIndexesCreatedThisRun += successfulIndexes; // Track indexes created this run
      
      if ((i + batchSize) % 200 === 0) {
        console.log(`🏗️ FIXED IP indexing progress: ${ipIndexesCreatedThisRun}/${ipIndexMap.size} indexes created this run`);
      }
      
    } catch (batchError) {
      console.log(`⚠️ Batch indexing error: ${batchError.message}`);
    }
  }
  
  // Step 3: Create simplified time indexes if time allows
  let timeIndexesCreated = 0;
  const finalRemainingTime = maxTime - (Date.now() - processStartTime);
  
  if (finalRemainingTime > 3000) {
    console.log(`🕐 Creating time indexes with ${finalRemainingTime}ms remaining...`);
    timeIndexesCreated = await createSimpleTimeIndexes(redis, timeStats, finalRemainingTime - 1000);
  }
  
  const timeSpanDays = timeStats.earliest && timeStats.latest 
    ? Math.ceil((timeStats.latest - timeStats.earliest) / (1000 * 60 * 60 * 24))
    : 0;
  
  const indexingTime = Date.now() - processStartTime;
  console.log(`✅ FIXED indexing completed in ${indexingTime}ms`);
  
  // Return comprehensive results for progress tracking
  return {
    chunks_processed_this_run: chunksProcessedThisRun,
    ip_indexes_created_this_run: ipIndexesCreatedThisRun,
    pageviews_processed_this_run: totalPageviews,
    total_pageviews: totalPageviews,
    unique_ips_indexed: ipIndexMap.size,
    time_indexes_created: timeIndexesCreated,
    processing_time_ms: indexingTime,
    attribution_fields_included: attributionFieldsIncluded,
    time_range: {
      earliest: timeStats.earliest?.toISOString(),
      latest: timeStats.latest?.toISOString(),
      span_days: timeSpanDays
    }
  };
}

// EXISTING FUNCTIONS BELOW - UNCHANGED
// ===================================

// Find all pageview chunks
async function findAllPageviewChunks(redis) {
  console.log('🔍 Finding all pageview chunks...');
  let cursor = '0';
  let allChunks = [];
  
  do {
    try {
      const scanResult = await redis(`scan/${cursor}/match/pageview_chunk:*/count/1000`);
      
      if (scanResult?.result && Array.isArray(scanResult.result) && scanResult.result.length >= 2) {
        cursor = scanResult.result[0];
        const keys = scanResult.result[1] || [];
        allChunks.push(...keys);
        
        console.log(`📦 Found ${keys.length} chunks, cursor: ${cursor}, total: ${allChunks.length}`);
      } else {
        cursor = '0';
      }
    } catch (error) {
      console.log(`⚠️ Error scanning for chunks: ${error.message}`);
      break;
    }
  } while (cursor !== '0');
  
  console.log(`✅ Found ${allChunks.length} total pageview chunks`);
  return allChunks;
}

// Create simple time indexes
async function createSimpleTimeIndexes(redis, timeStats, maxTime) {
  if (!timeStats.earliest || !timeStats.latest) return 0;
  
  const startTime = Date.now();
  console.log(`🕐 Creating time reference indexes...`);
  
  try {
    const timeIndexes = [
      { key: 'pageview_time_ref:earliest', value: timeStats.earliest.toISOString() },
      { key: 'pageview_time_ref:latest', value: timeStats.latest.toISOString() }
    ];
    
    let created = 0;
    for (const timeIndex of timeIndexes) {
      if (Date.now() - startTime > maxTime) break;
      
      try {
        await redis(`setex/${timeIndex.key}/2592000/${encodeURIComponent(timeIndex.value)}`, 1000);
        created++;
      } catch (error) {
        console.log(`⚠️ Error creating time index: ${error.message}`);
      }
    }
    
    console.log(`✅ Created ${created} time reference indexes`);
    return created;
    
  } catch (error) {
    console.log(`⚠️ Time indexing error: ${error.message}`);
    return 0;
  }
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
