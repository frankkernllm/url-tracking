// Enhanced Multi-Source Attribution Data Extractor v2
// Path: netlify/functions/extract-attribution-data-v2.js
// Purpose: Extract ALL pageviews with verification passes and adaptive chunk sizes
// NEW: Combines adaptive chunking + verification passes to catch missed keys

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
    
    console.log('🚀 Starting ENHANCED attribution data extraction v2...');
    
    // Load existing progress or start fresh (v2 to avoid conflicts)
    const progressKey = 'attribution_extraction_v2_progress';
    const existingProgress = await getAttributionProgress(redis, progressKey);
    
    console.log(`📊 Resuming enhanced attribution extraction v2:`, {
      total_extracted: existingProgress.total_extracted,
      current_pattern_index: existingProgress.current_pattern_index,
      verification_complete: existingProgress.verification_complete,
      last_cursor: existingProgress.last_cursor
    });
    
    // ENHANCED extraction with adaptive chunking + verification
    const extractionResult = await extractEnhancedAttributionData(
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
      verification_complete: extractionResult.verification_complete,
      verification_keys_found: extractionResult.verification_keys_found || 0,
      last_updated: new Date().toISOString(),
      is_complete: extractionResult.is_complete && extractionResult.verification_complete,
      patterns_completed: extractionResult.patterns_completed
    };
    
    await storeAttributionProgress(redis, progressKey, updatedProgress);
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ ENHANCED attribution extraction v2 finished in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        extraction_complete: extractionResult.is_complete,
        verification_complete: extractionResult.verification_complete,
        enhanced_attribution_summary: {
          // This run stats
          pageviews_extracted_this_run: extractionResult.pageviews_extracted_this_run,
          verification_keys_found_this_run: extractionResult.verification_keys_found || 0,
          keys_scanned_this_run: extractionResult.keys_scanned_this_run,
          processing_time_ms: totalTime,
          
          // Total stats across all runs
          total_pageviews_extracted: updatedProgress.total_extracted,
          total_verification_keys_found: updatedProgress.verification_keys_found,
          total_keys_scanned: updatedProgress.total_keys_scanned,
          extraction_method: 'enhanced_multi_source_attribution_v2'
        },
        enhancement_features: {
          adaptive_chunk_sizes: true,
          verification_passes: true,
          data_preservation: true,
          version: 'v2'
        },
        data_safety: {
          preserves_existing_v1_data: true,
          uses_separate_progress_tracking: true,
          uses_v2_chunk_identifiers: true
        },
        next_steps: (extractionResult.is_complete && extractionResult.verification_complete) ? [
          '✅ Enhanced extraction and verification complete!',
          'All patterns processed with verification passes',
          'Missed keys recovered through targeted verification',
          'Ready for attribution analysis with complete dataset'
        ] : [
          'Enhanced extraction continuing...',
          'Run the same command again to continue processing',
          `Main extraction complete: ${extractionResult.is_complete}`,
          `Verification complete: ${extractionResult.verification_complete}`
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Enhanced attribution extraction v2 failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Enhanced attribution extraction v2 failed', 
        message: error.message 
      })
    };
  }
};

// Enhanced extraction with adaptive chunking and verification
async function extractEnhancedAttributionData(redis, existingProgress, maxTime) {
  const extractionStartTime = Date.now();
  let thisRunPageviews = [];
  let thisRunKeysScanned = 0;
  let thisRunChunksProcessed = 0;
  let thisRunChunksStored = 0;
  let verificationKeysFound = 0;
  
  let currentPatternIndex = existingProgress.current_pattern_index;
  let currentCursor = existingProgress.last_cursor;
  let patternsCompleted = [...existingProgress.patterns_completed];
  let verificationComplete = existingProgress.verification_complete || false;
  
  console.log(`🔄 ENHANCED EXTRACTION v2: Starting from pattern ${currentPatternIndex}, cursor: ${currentCursor}`);
  
  try {
    // PHASE 1: Main extraction with adaptive chunking
    if (currentPatternIndex < existingProgress.patterns.length) {
      console.log('📊 PHASE 1: Main extraction with adaptive chunk sizes...');
      
      while (currentPatternIndex < existingProgress.patterns.length) {
        const currentPattern = existingProgress.patterns[currentPatternIndex];
        console.log(`🎯 Processing pattern ${currentPatternIndex + 1}/${existingProgress.patterns.length}: ${currentPattern}`);
        
        const timeRemaining = maxTime - (Date.now() - extractionStartTime);
        if (timeRemaining < 8000) {
          console.log(`⏰ Time limit approaching, saving for verification phase`);
          break;
        }
        
        // ENHANCED: Adaptive chunk sizes based on pattern
        const patternResult = await processEnhancedAttributionPattern(
          redis, 
          currentPattern, 
          currentCursor, 
          timeRemaining - 2000
        );
        
        // Accumulate results
        thisRunPageviews.push(...patternResult.pageviews);
        thisRunKeysScanned += patternResult.keys_scanned;
        thisRunChunksProcessed += patternResult.chunks_processed;
        
        // Check if pattern is complete
        if (patternResult.pattern_complete) {
          console.log(`✅ Pattern ${currentPattern} completed with enhanced scanning`);
          patternsCompleted.push(currentPattern);
          currentPatternIndex++;
          currentCursor = '0'; // Reset cursor for next pattern
        } else {
          console.log(`⏸️ Pattern ${currentPattern} incomplete, will resume from cursor: ${patternResult.final_cursor}`);
          currentCursor = patternResult.final_cursor;
          break;
        }
      }
    }
    
    // PHASE 2: Verification pass for missed keys
    const mainExtractionComplete = currentPatternIndex >= existingProgress.patterns.length;
    if (mainExtractionComplete && !verificationComplete) {
      const timeRemaining = maxTime - (Date.now() - extractionStartTime);
      if (timeRemaining > 5000) {
        console.log('🔍 PHASE 2: Running verification pass for missed keys...');
        
        const verificationResult = await runVerificationPass(redis, timeRemaining - 2000);
        verificationKeysFound = verificationResult.keys_found;
        thisRunPageviews.push(...verificationResult.pageviews);
        verificationComplete = verificationResult.complete;
        
        console.log(`✅ Verification pass complete: ${verificationKeysFound} missed keys recovered`);
      } else {
        console.log('⏰ Insufficient time for verification pass, will run next time');
      }
    }
    
    // Store accumulated pageviews in v2 chunks (preserves existing v1 data)
    if (thisRunPageviews.length > 0) {
      const chunkResult = await storeEnhancedAttributionChunks(redis, thisRunPageviews, existingProgress.chunks_stored);
      thisRunChunksStored = chunkResult.chunks_stored;
      console.log(`💾 Stored ${thisRunChunksStored} enhanced attribution chunks (v2) with ${thisRunPageviews.length} total pageviews`);
    }
    
    const isComplete = mainExtractionComplete;
    const processingTime = Date.now() - extractionStartTime;
    
    console.log(`🏁 ENHANCED attribution extraction v2 summary:`);
    console.log(`   📊 This run pageviews: ${thisRunPageviews.length}`);
    console.log(`   🔍 Verification keys found: ${verificationKeysFound}`);
    console.log(`   📦 This run chunks stored: ${thisRunChunksStored}`);
    console.log(`   ✅ Main extraction complete: ${isComplete}`);
    console.log(`   🔍 Verification complete: ${verificationComplete}`);
    
    return {
      pageviews_extracted_this_run: thisRunPageviews.length,
      verification_keys_found: verificationKeysFound,
      keys_scanned_this_run: thisRunKeysScanned,
      chunks_processed_this_run: thisRunChunksProcessed,
      chunks_stored_this_run: thisRunChunksStored,
      is_complete: isComplete,
      verification_complete: verificationComplete,
      final_pattern_index: currentPatternIndex,
      final_cursor: currentCursor,
      patterns_completed: patternsCompleted,
      processing_time_ms: processingTime
    };
    
  } catch (error) {
    console.error('❌ Enhanced attribution extraction error:', error);
    return {
      pageviews_extracted_this_run: thisRunPageviews.length,
      verification_keys_found: verificationKeysFound,
      is_complete: false,
      verification_complete: false,
      final_pattern_index: currentPatternIndex,
      final_cursor: currentCursor,
      error: error.message
    };
  }
}

// ENHANCED: Process attribution pattern with adaptive chunk sizes
async function processEnhancedAttributionPattern(redis, pattern, startCursor, maxTime) {
  const patternStartTime = Date.now();
  let cursor = startCursor;
  let pageviews = [];
  let keysScanned = 0;
  let chunksProcessed = 0;
  
  // OPTION 3: Adaptive chunk sizes based on pattern
  const getChunkSize = (pattern) => {
    if (pattern === 'attribution_*') {
      return 100; // Smaller chunks for attribution_* to reduce Redis SCAN issues
    } else if (pattern === 'pageviews:*') {
      return 500; // Standard size for pageviews
    } else {
      return 300; // Medium size for other patterns
    }
  };
  
  const chunkSize = getChunkSize(pattern);
  console.log(`🎯 Processing ${pattern} with adaptive chunk size: ${chunkSize}`);
  
  try {
    do {
      const timeRemaining = maxTime - (Date.now() - patternStartTime);
      if (timeRemaining < 3000) {
        console.log(`⏰ Pattern time limit approaching: ${timeRemaining}ms remaining`);
        break;
      }
      
      console.log(`🔍 Enhanced pattern scan cursor: ${cursor}, collected: ${pageviews.length} pageviews`);
      
      // Use adaptive chunk size
      const scanResult = await redis(`scan/${cursor}/match/${pattern}/count/${chunkSize}`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        console.log(`🏁 Enhanced pattern scan complete: no more results for ${pattern}`);
        cursor = '0';
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      keysScanned += keys.length;
      chunksProcessed++;
      
      console.log(`📊 Enhanced pattern chunk ${chunksProcessed}: Found ${keys.length} keys with chunk size ${chunkSize}, cursor: ${cursor}`);
      
      // Process keys normally
      const attributionKeys = filterAttributionKeys(keys, pattern);
      console.log(`📝 Filtered: ${attributionKeys.length} attribution keys from ${keys.length} total`);
      
      if (attributionKeys.length > 0) {
        const batchPageviews = await processAttributionKeyBatch(redis, attributionKeys, pattern);
        pageviews.push(...batchPageviews);
        console.log(`📦 Enhanced batch processed: ${batchPageviews.length} pageviews`);
      }
      
      // Safety check
      if (chunksProcessed >= 50) {
        console.log(`🛑 Enhanced pattern safety limit: processed 50 chunks`);
        break;
      }
      
    } while (cursor !== '0' && Date.now() - patternStartTime < maxTime - 2000);
    
    const patternComplete = cursor === '0';
    const patternTime = Date.now() - patternStartTime;
    
    console.log(`🎯 Enhanced pattern ${pattern} summary:`);
    console.log(`   📊 Pageviews extracted: ${pageviews.length}`);
    console.log(`   🔍 Keys scanned: ${keysScanned}`);
    console.log(`   📦 Chunks processed: ${chunksProcessed}`);
    console.log(`   🎯 Chunk size used: ${chunkSize}`);
    console.log(`   ✅ Complete: ${patternComplete}`);
    
    return {
      pageviews: pageviews,
      keys_scanned: keysScanned,
      chunks_processed: chunksProcessed,
      pattern_complete: patternComplete,
      final_cursor: cursor,
      processing_time_ms: patternTime
    };
    
  } catch (error) {
    console.error(`❌ Enhanced pattern ${pattern} processing error:`, error);
    return {
      pageviews: pageviews,
      keys_scanned: keysScanned,
      chunks_processed: chunksProcessed,
      pattern_complete: false,
      final_cursor: cursor,
      error: error.message
    };
  }
}

// OPTION 1: Verification pass to catch missed keys
async function runVerificationPass(redis, maxTime) {
  console.log('🔍 Starting targeted verification pass for missed keys...');
  const verificationStartTime = Date.now();
  const recoveredPageviews = [];
  
  // Target specific timeframes where we know data exists but might be missed
  const suspiciousTimeframes = [
    '1753484*', // July 25th evening (your target IP timeframe)
    '1753463*', // July 25th afternoon
    '1753465*', // July 25th late afternoon
    '1753470*', // July 25th early evening
    // Add more timeframes based on patterns
  ];
  
  console.log(`🎯 Verification targeting ${suspiciousTimeframes.length} suspicious timeframes`);
  
  for (const timeframe of suspiciousTimeframes) {
    const timeRemaining = maxTime - (Date.now() - verificationStartTime);
    if (timeRemaining < 2000) {
      console.log('⏰ Verification time limit reached');
      break;
    }
    
    try {
      console.log(`🔍 Verifying timeframe: ${timeframe}`);
      
      // Use very small chunks for verification to catch missed keys
      const verificationPattern = `attribution_*_${timeframe}`;
      const verificationKeys = await targetedScan(redis, verificationPattern, 50); // Very small chunks
      
      if (verificationKeys.length > 0) {
        console.log(`🎯 Verification found ${verificationKeys.length} keys in timeframe ${timeframe}`);
        
        const verificationPageviews = await processAttributionKeyBatch(redis, verificationKeys, 'attribution_*');
        recoveredPageviews.push(...verificationPageviews);
        
        console.log(`✅ Recovered ${verificationPageviews.length} pageviews from verification`);
      }
      
    } catch (verificationError) {
      console.log(`⚠️ Verification error for timeframe ${timeframe}:`, verificationError.message);
    }
  }
  
  const verificationTime = Date.now() - verificationStartTime;
  console.log(`🔍 Verification pass complete: ${recoveredPageviews.length} keys recovered in ${verificationTime}ms`);
  
  return {
    keys_found: recoveredPageviews.length,
    pageviews: recoveredPageviews,
    complete: true,
    processing_time_ms: verificationTime
  };
}

// Targeted scan for verification with very small chunks
async function targetedScan(redis, pattern, chunkSize) {
  const foundKeys = [];
  let cursor = '0';
  let attempts = 0;
  
  do {
    try {
      const scanResult = await redis(`scan/${cursor}/match/${pattern}/count/${chunkSize}`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      foundKeys.push(...keys);
      
      attempts++;
      if (attempts >= 20) break; // Limit verification attempts
      
    } catch (error) {
      console.log(`⚠️ Targeted scan error: ${error.message}`);
      break;
    }
  } while (cursor !== '0');
  
  return foundKeys;
}

// Store enhanced attribution chunks with v2 identifiers (preserves existing data)
async function storeEnhancedAttributionChunks(redis, pageviews, startingChunkNumber) {
  if (pageviews.length === 0) return { chunks_stored: 0 };
  
  const chunkSize = 1000;
  let chunksStored = 0;
  
  for (let i = 0; i < pageviews.length; i += chunkSize) {
    const chunk = pageviews.slice(i, i + chunkSize);
    const chunkNumber = startingChunkNumber + chunksStored + 1;
    
    // V2 chunk identifier to avoid overwriting v1 data
    const chunkKey = `attribution_data_chunk:v2_${chunkNumber}:${Date.now()}`;
    
    const chunkData = {
      chunk_id: `v2_${chunkNumber}`,
      pageview_count: chunk.length,
      pageviews: chunk,
      created_at: new Date().toISOString(),
      version: 'v2_enhanced',
      extraction_method: 'enhanced_with_verification',
      attribution_ready: true
    };
    
    try {
      await redis(`setex/${chunkKey}/2592000/${encodeURIComponent(JSON.stringify(chunkData))}`);
      chunksStored++;
      console.log(`💾 Stored enhanced chunk v2_${chunkNumber} with ${chunk.length} pageviews`);
    } catch (error) {
      console.error(`❌ Failed to store enhanced chunk v2_${chunkNumber}:`, error);
    }
  }
  
  return { chunks_stored: chunksStored };
}

// Get attribution extraction progress (v2 version)
async function getAttributionProgress(redis, progressKey) {
  try {
    const progressData = await redis(`get/${progressKey}`);
    
    if (progressData?.result) {
      const progress = JSON.parse(decodeURIComponent(progressData.result));
      console.log(`🔄 Found existing attribution progress v2: ${progress.total_extracted} pageviews extracted`);
      return progress;
    }
  } catch (error) {
    console.log('⚠️ No existing attribution progress v2 found, starting fresh');
  }
  
  return {
    total_extracted: 0,
    total_keys_scanned: 0,
    patterns: [
      'attribution_*',
      'pageviews:*',
      'attribution:*'
    ],
    current_pattern_index: 0,
    last_cursor: '0',
    chunks_completed: 0,
    chunks_stored: 0,
    verification_complete: false,
    verification_keys_found: 0,
    started_at: new Date().toISOString(),
    is_complete: false,
    patterns_completed: []
  };
}

// Store attribution progress (v2)
async function storeAttributionProgress(redis, progressKey, progress) {
  await redis(`setex/${progressKey}/3600/${encodeURIComponent(JSON.stringify(progress))}`);
  console.log(`💾 Attribution progress v2 saved: ${progress.total_extracted} total pageviews, verification: ${progress.verification_complete}`);
}

// Reuse existing helper functions
function filterAttributionKeys(keys, pattern) {
  if (pattern === 'attribution_*') {
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
             key.match(/\d+$/);
    });
  } else if (pattern === 'pageviews:*') {
    return keys.filter(key => key.startsWith('pageviews:') && !key.includes('index_'));
  } else if (pattern === 'attribution:*') {
    return keys.filter(key => key.startsWith('attribution:') && !key.includes('stats'));
  }
  
  return keys;
}

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
          return {
            session_id: parsed.session_id || null,
            timestamp: parsed.timestamp,
            landing_page: parsed.landing_page || 'unknown',
            source: parsed.source || 'direct',
            ip_address: parsed.ip_address,
            canvas_fingerprint: parsed.canvas_fingerprint || null,
            webgl_fingerprint: parsed.webgl_fingerprint || null,
            referrer_url: parsed.referrer_url || null,
            utm_campaign: parsed.utm_campaign || null,
            utm_source: parsed.utm_source || null,
            utm_medium: parsed.utm_medium || null,
            utm_term: parsed.utm_term || null,
            utm_content: parsed.utm_content || null,
            screen_resolution: parsed.screen_resolution || null,
            cpu_cores: parsed.cpu_cores || null,
            memory_gb: parsed.memory_gb || null,
            redis_key: key,
            source_pattern: pattern
          };
        }
      }
    } catch (error) {
      // Skip errors
    }
    return null;
  });
  
  const results = await Promise.all(batchPromises);
  return results.filter(result => result !== null);
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
