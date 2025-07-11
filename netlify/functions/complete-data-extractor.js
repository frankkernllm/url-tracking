// complete-data-extractor.js - Full System Extraction Orchestrator
// Path: netlify/functions/complete-data-extractor.js
// Purpose: Orchestrates complete extraction of both pageviews and conversions

const headers = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
  'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
  'Content-Type': 'application/json'
};

exports.handler = async (event, context) => {
  context.callbackWaitsForEmptyEventLoop = false;
  
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers };
  }

  try {
    console.log('🚀 COMPLETE-DATA-EXTRACTOR: Starting full system extraction');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max
    
    // Parse request body
    const body = event.body ? JSON.parse(event.body) : {};
    const forceRestart = body.force_restart || false;
    const extractPageviews = body.extract_pageviews !== false; // Default true
    const extractConversions = body.extract_conversions !== false; // Default true
    
    console.log(`🎯 Configuration: pageviews=${extractPageviews}, conversions=${extractConversions}, restart=${forceRestart}`);
    
    const results = {
      pageview_extraction: null,
      conversion_extraction: null,
      total_processing_time_ms: 0,
      system_ready: false
    };
    
    // STEP 1: Extract Pageviews (if requested)
    if (extractPageviews) {
      console.log('📄 STEP 1: Starting pageview extraction...');
      const pageviewStart = Date.now();
      
      try {
        results.pageview_extraction = await runPageviewExtraction(forceRestart);
        results.pageview_extraction.processing_time_ms = Date.now() - pageviewStart;
        console.log(`✅ Pageview extraction: ${results.pageview_extraction.success ? 'SUCCESS' : 'PARTIAL'}`);
      } catch (pageviewError) {
        console.error('❌ Pageview extraction failed:', pageviewError);
        results.pageview_extraction = {
          success: false,
          error: pageviewError.message,
          processing_time_ms: Date.now() - pageviewStart
        };
      }
    }
    
    // STEP 2: Extract Conversions (if requested and we have time)
    if (extractConversions && Date.now() - startTime < maxProcessingTime - 10000) {
      console.log('💰 STEP 2: Starting conversion extraction...');
      const conversionStart = Date.now();
      
      try {
        results.conversion_extraction = await runConversionExtraction();
        results.conversion_extraction.processing_time_ms = Date.now() - conversionStart;
        console.log(`✅ Conversion extraction: ${results.conversion_extraction.success ? 'SUCCESS' : 'PARTIAL'}`);
      } catch (conversionError) {
        console.error('❌ Conversion extraction failed:', conversionError);
        results.conversion_extraction = {
          success: false,
          error: conversionError.message,
          processing_time_ms: Date.now() - conversionStart
        };
      }
    } else if (extractConversions) {
      console.log('⏰ Insufficient time for conversion extraction');
    }
    
    // STEP 3: System Status Check
    const pageviewsReady = !extractPageviews || (results.pageview_extraction?.success);
    const conversionsReady = !extractConversions || (results.conversion_extraction?.success);
    results.system_ready = pageviewsReady && conversionsReady;
    
    results.total_processing_time_ms = Date.now() - startTime;
    
    console.log(`🏁 Complete extraction finished in ${results.total_processing_time_ms}ms`);
    console.log(`📊 System ready: ${results.system_ready}`);
    
    // Determine next steps
    const nextSteps = [];
    
    if (!pageviewsReady && results.pageview_extraction?.continue_command) {
      nextSteps.push({
        action: 'continue_pageview_extraction',
        command: results.pageview_extraction.continue_command.curl
      });
    }
    
    if (!conversionsReady && results.conversion_extraction?.continue_command) {
      nextSteps.push({
        action: 'continue_conversion_extraction', 
        command: results.conversion_extraction.continue_command.curl
      });
    }
    
    if (results.system_ready) {
      nextSteps.push({
        action: 'test_fast_analytics',
        command: 'curl "https://trackingojoy.netlify.app/.netlify/functions/fast-analytics?start_date=2025-07-10&end_date=2025-07-11" -H "X-API-Key: YOUR_API_KEY"'
      });
    }
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        system_status: results.system_ready ? 'READY' : 'PARTIAL',
        message: results.system_ready ? 
          'System fully extracted and indexed! Dashboard should now be lightning fast.' :
          'Partial extraction completed. Continue extraction for full system readiness.',
        
        extraction_results: {
          pageviews: results.pageview_extraction,
          conversions: results.conversion_extraction,
          total_time_ms: results.total_processing_time_ms
        },
        
        system_readiness: {
          pageviews_ready: pageviewsReady,
          conversions_ready: conversionsReady,
          fast_analytics_ready: results.system_ready,
          dashboard_ready: results.system_ready
        },
        
        next_steps: nextSteps,
        
        performance_summary: {
          pageview_extraction_time: results.pageview_extraction?.processing_time_ms || 0,
          conversion_extraction_time: results.conversion_extraction?.processing_time_ms || 0,
          total_extraction_time: results.total_processing_time_ms,
          system_extraction_version: 'complete_v1.0'
        }
      })
    };
    
  } catch (error) {
    console.error('❌ Complete data extraction failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Complete data extraction failed', 
        message: error.message,
        timestamp: new Date().toISOString()
      })
    };
  }
};

// Run pageview extraction using existing auto-chunked-extractor
async function runPageviewExtraction(forceRestart) {
  console.log('📄 Running pageview extraction...');
  
  const url = 'https://trackingojoy.netlify.app/.netlify/functions/auto-chunked-extractor';
  const payload = forceRestart ? { force_restart: true } : {};
  
  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(20000) // 20 second timeout
    });

    if (!response.ok) {
      throw new Error(`Pageview extraction HTTP ${response.status}: ${response.statusText}`);
    }

    const result = await response.json();
    
    return {
      success: result.success && result.status === 'COMPLETED',
      status: result.status,
      extraction_summary: result.extraction_summary,
      indexing: result.indexing,
      continue_command: result.continue_command,
      partial: result.status === 'PARTIAL'
    };

  } catch (error) {
    console.error('❌ Pageview extraction error:', error);
    throw error;
  }
}

// Run conversion extraction using new extract-conversions-chunked
async function runConversionExtraction() {
  console.log('💰 Running conversion extraction...');
  
  const url = 'https://trackingojoy.netlify.app/.netlify/functions/extract-conversions-chunked';
  
  let cursor = '0';
  let totalExtracted = 0;
  let totalIndexes = 0;
  let attempts = 0;
  const maxAttempts = 5; // Limit attempts to avoid infinite loops
  
  try {
    // Continue extraction until complete or max attempts
    while (cursor !== '0' && attempts < maxAttempts) {
      attempts++;
      console.log(`💰 Conversion extraction attempt ${attempts}, cursor: ${cursor}`);
      
      const payload = cursor === '0' ? {} : { start_cursor: cursor };
      
      const response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(15000) // 15 second timeout per chunk
      });

      if (!response.ok) {
        throw new Error(`Conversion extraction HTTP ${response.status}: ${response.statusText}`);
      }

      const result = await response.json();
      
      if (!result.success) {
        throw new Error('Conversion extraction returned success: false');
      }
      
      // Update totals
      totalExtracted += result.chunk_summary.conversions_extracted_this_chunk || 0;
      totalIndexes += result.indexing_summary.total_indexes_created || 0;
      cursor = result.chunk_summary.final_cursor;
      
      console.log(`💰 Chunk ${attempts}: ${result.chunk_summary.conversions_extracted_this_chunk} conversions, cursor: ${cursor}`);
      
      // Break if extraction is complete
      if (result.chunk_summary.is_complete) {
        console.log('✅ Conversion extraction complete!');
        break;
      }
      
      // Small delay between chunks
      await new Promise(resolve => setTimeout(resolve, 500));
    }
    
    const isComplete = cursor === '0' || attempts >= maxAttempts;
    
    return {
      success: isComplete,
      total_conversions_extracted: totalExtracted,
      total_indexes_created: totalIndexes,
      chunks_processed: attempts,
      extraction_complete: isComplete,
      final_cursor: cursor,
      continue_command: isComplete ? null : {
        curl: `curl -X POST https://trackingojoy.netlify.app/.netlify/functions/extract-conversions-chunked -H "Content-Type: application/json" -d '{"start_cursor":"${cursor}"}'`
      }
    };

  } catch (error) {
    console.error('❌ Conversion extraction error:', error);
    throw error;
  }
}
