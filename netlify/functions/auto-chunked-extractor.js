// auto-chunked-extractor.js - Automated Continuous Chunked Extraction
// Path: netlify/functions/auto-chunked-extractor.js
// Purpose: Automatically continues extract-pageviews-chunked until complete

const headers = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
  'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
  'Content-Type': 'application/json'
};

exports.handler = async (event, context) => {
  // Handle preflight requests
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers };
  }

  try {
    console.log('🚀 AUTO-CHUNKED-EXTRACTOR: Starting automated continuous extraction');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max (5 second buffer for Netlify 30s limit)

    // Parse request body for resume capability
    const requestBody = event.body ? JSON.parse(event.body) : {};
    const startCursor = requestBody.start_cursor || '0';
    const forceRestart = requestBody.force_restart || false;

    console.log(`📍 Starting from cursor: ${startCursor}`);
    console.log(`🔄 Force restart: ${forceRestart}`);

    // Check if extraction is already in progress (unless forced restart)
    if (!forceRestart) {
      const existingProgress = await getExtractionProgress();
      if (existingProgress && existingProgress.in_progress) {
        console.log('⚠️ Extraction already in progress, resuming...');
        return {
          statusCode: 200,
          headers,
          body: JSON.stringify({
            success: true,
            status: 'ALREADY_IN_PROGRESS',
            message: 'Extraction already running. Use force_restart:true to restart.',
            existing_progress: existingProgress,
            resume_command: 'curl -X POST https://trackingojoy.netlify.app/.netlify/functions/auto-chunked-extractor'
          })
        };
      }
    }

    // Mark extraction as in progress
    await markExtractionInProgress(true);

    let cursor = startCursor;
    let totalExtracted = 0;
    let totalKeysScanned = 0;
    let chunksCompleted = 0;
    let allChunksSuccessful = true;
    const chunkResults = [];

    console.log('🔄 Starting continuous chunked extraction loop...');

    // Continue extracting until cursor is "0" or we hit time limit
    while (cursor !== '0' && Date.now() - startTime < maxProcessingTime - 3000) {
      try {
        console.log(`📦 Processing chunk ${chunksCompleted + 1} with cursor: ${cursor}`);

        // Call the working extract-pageviews-chunked function
        const chunkResult = await callChunkedExtraction(cursor);

        if (chunkResult.success) {
          const chunkSummary = chunkResult.chunk_summary;
          
          totalExtracted += chunkSummary.pageviews_extracted_this_chunk || 0;
          totalKeysScanned += chunkSummary.keys_scanned_this_chunk || 0;
          chunksCompleted++;

          chunkResults.push({
            chunk_number: chunksCompleted,
            pageviews: chunkSummary.pageviews_extracted_this_chunk || 0,
            keys_scanned: chunkSummary.keys_scanned_this_chunk || 0,
            processing_time_ms: chunkSummary.processing_time_ms || 0,
            cursor_start: cursor,
            cursor_end: chunkSummary.final_cursor
          });

          cursor = chunkSummary.final_cursor;
          
          console.log(`✅ Chunk ${chunksCompleted}: ${chunkSummary.pageviews_extracted_this_chunk} pageviews, cursor: ${cursor}`);

          // Check if extraction is complete
          if (chunkSummary.is_complete || cursor === '0') {
            console.log('🎉 Extraction complete! All chunks processed.');
            break;
          }

          // Update progress every few chunks
          if (chunksCompleted % 3 === 0) {
            await updateExtractionProgress({
              chunks_completed: chunksCompleted,
              total_extracted: totalExtracted,
              current_cursor: cursor,
              last_updated: new Date().toISOString()
            });
          }

          // Small delay between chunks to avoid overwhelming the system
          await new Promise(resolve => setTimeout(resolve, 200));

        } else {
          console.error(`❌ Chunk ${chunksCompleted + 1} failed:`, chunkResult.error);
          allChunksSuccessful = false;
          
          // Try to continue with next chunk if we have a cursor
          if (chunkResult.chunk_summary?.final_cursor && chunkResult.chunk_summary.final_cursor !== cursor) {
            cursor = chunkResult.chunk_summary.final_cursor;
            console.log(`🔄 Continuing with cursor from failed chunk: ${cursor}`);
          } else {
            console.log('❌ Cannot continue extraction, no valid cursor from failed chunk');
            break;
          }
        }

      } catch (chunkError) {
        console.error(`❌ Chunk processing error:`, chunkError);
        allChunksSuccessful = false;
        break;
      }
    }

    const extractionComplete = cursor === '0';
    const timeRemaining = maxProcessingTime - (Date.now() - startTime);

    console.log(`📊 Extraction summary: ${totalExtracted} pageviews from ${chunksCompleted} chunks`);

    // Build indexes if extraction is complete and we have time
    let indexResult = null;
    if (extractionComplete && timeRemaining > 5000) {
      console.log('🏗️ Extraction complete, building indexes...');
      try {
        indexResult = await buildIndexesFromChunks();
        console.log('✅ Indexes built successfully');
      } catch (indexError) {
        console.error('❌ Index building failed:', indexError);
        indexResult = { error: indexError.message };
      }
    } else if (!extractionComplete) {
      console.log(`⏰ Time limit reached, extraction not complete. Resume with cursor: ${cursor}`);
    }

    // Mark extraction as complete (or paused if not finished)
    await markExtractionInProgress(false);
    
    // Store final progress
    await updateExtractionProgress({
      chunks_completed: chunksCompleted,
      total_extracted: totalExtracted,
      current_cursor: cursor,
      extraction_complete: extractionComplete,
      completed_at: extractionComplete ? new Date().toISOString() : null,
      last_updated: new Date().toISOString()
    });

    const totalTime = Date.now() - startTime;

    // Determine status and next action
    const status = extractionComplete ? 'COMPLETED' : 'PARTIAL';
    const nextAction = extractionComplete ? 
      'Extraction complete! Run analytics to see results.' :
      `Continue extraction with cursor: ${cursor}`;

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: allChunksSuccessful,
        status: status,
        extraction_summary: {
          chunks_processed: chunksCompleted,
          total_pageviews_extracted: totalExtracted,
          total_keys_scanned: totalKeysScanned,
          processing_time_ms: totalTime,
          extraction_complete: extractionComplete,
          all_chunks_successful: allChunksSuccessful
        },
        indexing: indexResult ? {
          indexes_built: !indexResult.error,
          index_result: indexResult
        } : {
          indexes_built: false,
          reason: extractionComplete ? 'Insufficient time remaining' : 'Extraction not complete'
        },
        next_action: nextAction,
        continue_command: extractionComplete ? null : {
          message: "Run this command to continue extraction:",
          curl: `curl -X POST https://trackingojoy.netlify.app/.netlify/functions/auto-chunked-extractor -H "Content-Type: application/json" -d '{"start_cursor":"${cursor}"}'`
        },
        chunk_details: chunkResults,
        performance: {
          pageviews_per_second: totalTime > 0 ? Math.round((totalExtracted / totalTime) * 1000) : 0,
          chunks_per_second: totalTime > 0 ? Math.round((chunksCompleted / totalTime) * 1000 * 10) / 10 : 0,
          avg_chunk_time_ms: chunksCompleted > 0 ? Math.round(totalTime / chunksCompleted) : 0
        }
      })
    };

  } catch (error) {
    console.error('❌ Auto-chunked extraction failed:', error);
    
    // Mark extraction as not in progress on error
    try {
      await markExtractionInProgress(false);
    } catch (markError) {
      console.error('❌ Failed to mark extraction as not in progress:', markError);
    }

    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({
        success: false,
        error: 'Auto-chunked extraction failed',
        message: error.message,
        timestamp: new Date().toISOString()
      })
    };
  }
};

// Call the extract-pageviews-chunked function
async function callChunkedExtraction(cursor) {
  const url = 'https://trackingojoy.netlify.app/.netlify/functions/extract-pageviews-chunked';
  
  const payload = cursor === '0' ? {} : { start_cursor: cursor, pattern: "attribution_*" };
  
  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(15000) // 15 second timeout per chunk
    });

    if (!response.ok) {
      throw new Error(`Chunked extraction HTTP ${response.status}: ${response.statusText}`);
    }

    const result = await response.json();
    return result;

  } catch (error) {
    console.error('❌ Failed to call chunked extraction:', error);
    return {
      success: false,
      error: error.message,
      chunk_summary: { final_cursor: cursor } // Return original cursor to avoid infinite loop
    };
  }
}

// Build indexes from extracted chunks
async function buildIndexesFromChunks() {
  const url = 'https://trackingojoy.netlify.app/.netlify/functions/build-indexes-complete';
  
  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
      signal: AbortSignal.timeout(10000) // 10 second timeout for index building
    });

    if (!response.ok) {
      throw new Error(`Index building HTTP ${response.status}: ${response.statusText}`);
    }

    const result = await response.json();
    return result;

  } catch (error) {
    console.error('❌ Failed to build indexes:', error);
    throw error;
  }
}

// Progress tracking functions
async function getExtractionProgress() {
  try {
    // This would typically read from Redis or a persistent store
    // For now, return null (no progress tracking)
    return null;
  } catch (error) {
    console.error('❌ Failed to get extraction progress:', error);
    return null;
  }
}

async function markExtractionInProgress(inProgress) {
  try {
    // This would typically write to Redis
    console.log(`📝 Marking extraction in progress: ${inProgress}`);
  } catch (error) {
    console.error('❌ Failed to mark extraction progress:', error);
  }
}

async function updateExtractionProgress(progress) {
  try {
    // This would typically write progress to Redis
    console.log('📝 Updating extraction progress:', progress);
  } catch (error) {
    console.error('❌ Failed to update extraction progress:', error);
  }
}
