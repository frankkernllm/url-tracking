// scheduled-extraction.js - Netlify function
// Automatically triggers extract-pageviews-chunked.js every hour to keep indexes fresh

const headers = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Content-Type': 'application/json'
};

exports.handler = async (event, context) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers };
  }

  // Validate API key
  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  if (apiKey !== process.env.OJOY_API_KEY) {
    return {
      statusCode: 401,
      headers,
      body: JSON.stringify({ error: 'Invalid API key' })
    };
  }

  try {
    console.log('🔄 SCHEDULED EXTRACTION: Starting automated pageview extraction');
    const startTime = Date.now();

    // Check if extraction is already running
    const isRunning = await checkExtractionStatus();
    if (isRunning) {
      console.log('⏸️ Extraction already in progress, skipping this run');
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          status: 'skipped',
          reason: 'Extraction already in progress',
          timestamp: new Date().toISOString()
        })
      };
    }

    // Run the complete extraction process
    const extractionResult = await runCompleteExtraction();
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ EXTRACTION COMPLETE: ${totalTime/1000}s total`);

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        status: 'completed',
        duration_seconds: Math.round(totalTime / 1000),
        extraction_result: extractionResult,
        timestamp: new Date().toISOString(),
        next_run: new Date(Date.now() + 60*60*1000).toISOString() // +1 hour
      })
    };

  } catch (error) {
    console.error('❌ Scheduled extraction failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({
        error: 'Extraction failed',
        details: error.message,
        timestamp: new Date().toISOString()
      })
    };
  }
};

// Check if extraction is currently running
async function checkExtractionStatus() {
  try {
    const response = await fetch(`${process.env.URL}/.netlify/functions/extract-pageviews-chunked`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': process.env.OJOY_API_KEY
      },
      body: JSON.stringify({ action: 'status' })
    });

    if (response.ok) {
      const status = await response.json();
      return status.extraction_in_progress || false;
    }
    return false;
  } catch (error) {
    console.warn('⚠️ Could not check extraction status:', error.message);
    return false;
  }
}

// Run complete extraction process (mimics extraction-helper.js logic)
async function runCompleteExtraction() {
  console.log('📊 Starting pageview extraction process...');
  
  let cursor = '0';
  let totalExtracted = 0;
  let completedChunks = 0;
  const maxChunks = 50; // Safety limit
  
  // Step 1: Extract all pageview chunks
  do {
    console.log(`📦 Processing chunk ${completedChunks + 1} (cursor: ${cursor})`);
    
    const extractResponse = await fetch(`https://trackingojoy.netlify.app/.netlify/functions/extract-pageviews-chunked`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': process.env.OJOY_API_KEY
      },
      body: JSON.stringify({ 
        start_cursor: cursor !== '0' ? cursor : undefined,
        chunk_size: 2000
      })
    });

    if (!extractResponse.ok) {
      throw new Error(`Extraction chunk failed: ${extractResponse.status}`);
    }

    const chunkResult = await extractResponse.json();
    console.log(`✅ Chunk ${completedChunks + 1}: ${chunkResult.pageviews_extracted || 0} pageviews`);
    
    cursor = chunkResult.next_cursor || '0';
    totalExtracted += chunkResult.pageviews_extracted || 0;
    completedChunks++;
    
    // Safety delay between chunks
    if (cursor !== '0') {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
  } while (cursor !== '0' && completedChunks < maxChunks);

  console.log(`📊 Extraction complete: ${totalExtracted} pageviews in ${completedChunks} chunks`);

  // Step 2: Build indexes from extracted chunks
  console.log('🔨 Building indexes from extracted chunks...');
  
  const indexResponse = await fetch(`https://trackingojoy.netlify.app/.netlify/functions/build-indexes-complete`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-API-Key': process.env.OJOY_API_KEY
    },
    body: JSON.stringify({})
  });

  if (!indexResponse.ok) {
    throw new Error(`Index building failed: ${indexResponse.status}`);
  }

  const indexResult = await indexResponse.json();
  console.log(`✅ Indexes built: ${indexResult.unique_ips || 0} IPs, ${indexResult.session_ids || 0} sessions`);

  // Step 3: Test query system
  console.log('🧪 Testing query system...');
  
  const testResponse = await fetch(`https://trackingojoy.netlify.app/.netlify/functions/query-pageviews-enhanced`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-API-Key': process.env.OJOY_API_KEY
    },
    body: JSON.stringify({
      conversion_timestamp: new Date().toISOString(),
      ips_to_check: ['test'],
      window_hours: 24
    })
  });

  const testResult = testResponse.ok ? await testResponse.json() : { error: 'Test failed' };
  console.log(`🧪 Query test: ${testResponse.ok ? 'PASSED' : 'FAILED'}`);

  return {
    total_pageviews_extracted: totalExtracted,
    chunks_processed: completedChunks,
    indexes_built: true,
    unique_ips: indexResult.unique_ips || 0,
    session_ids: indexResult.session_ids || 0,
    query_test_passed: testResponse.ok,
    extraction_complete: cursor === '0'
  };
}
