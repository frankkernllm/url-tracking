// Simple V2 Conversion Batch Processor
// Path: netlify/functions/simple-batch-v2-processor.js
// Purpose: Process V2 conversions using global index (simpler approach)
//
// PARAMETERS:
// - start_index: Starting conversion index (required)
// - count: Number of conversions to process (default: 10)
// - skip_existing: Skip if attribution already exists (default: true)
// 
// Example usage:
// { "start_index": 0, "count": 25, "skip_existing": true }
// { "start_index": 25, "count": 25, "skip_existing": true }

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
    const startTime = Date.now();
    
    // Parse request parameters
    const requestData = JSON.parse(event.body || '{}');
    const { start_index, count = 10, skip_existing = true } = requestData;
    
    if (start_index === undefined) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({
          error: 'Missing required parameter: start_index',
          example: { start_index: 0, count: 10, skip_existing: true }
        })
      };
    }
    
    console.log(`🚀 Simple V2 batch processor: processing ${count} conversions starting from index ${start_index}`);
    
    // Process conversions using global index approach
    const results = [];
    let successCount = 0;
    let skipCount = 0;
    let errorCount = 0;
    
    for (let i = 0; i < count; i++) {
      const conversionIndex = start_index + i;
      console.log(`📊 Processing global conversion index ${conversionIndex} (${i + 1}/${count})`);
      
      try {
        // Call V2 attribution engine with global index
        const attributionResponse = await fetch(`${process.env.URL}/.netlify/functions/multi-touch-attribution-v2`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            conversion_index: conversionIndex,
            skip_existing_check: !skip_existing,
            force_debug: false
          })
        });
        
        const attributionResult = await attributionResponse.json();
        
        if (attributionResponse.status === 200 && attributionResult.success) {
          if (attributionResult.message === 'V2 Attribution already exists' && skip_existing) {
            console.log(`⏭️ Skipped conversion index ${conversionIndex} - already exists`);
            skipCount++;
            results.push({
              conversion_index: conversionIndex,
              status: 'skipped',
              reason: 'already_exists'
            });
          } else {
            console.log(`✅ Successfully processed conversion index ${conversionIndex}`);
            successCount++;
            results.push({
              conversion_index: conversionIndex,
              status: 'success',
              email: attributionResult.attribution_result?.conversion?.email,
              touchpoints: attributionResult.attribution_result?.attribution_summary?.total_touchpoints || 0
            });
          }
        } else if (attributionResponse.status === 404) {
          console.log(`🏁 Reached end of conversions at index ${conversionIndex}`);
          results.push({
            conversion_index: conversionIndex,
            status: 'end_of_data',
            message: 'No more conversions available'
          });
          break;
        } else {
          console.log(`❌ Failed to process conversion index ${conversionIndex}: ${attributionResult.error}`);
          errorCount++;
          results.push({
            conversion_index: conversionIndex,
            status: 'error',
            error: attributionResult.error || 'Unknown error'
          });
        }
        
      } catch (error) {
        console.log(`❌ Error processing conversion index ${conversionIndex}: ${error.message}`);
        errorCount++;
        results.push({
          conversion_index: conversionIndex,
          status: 'error',
          error: error.message
        });
      }
      
      // Brief pause between requests
      if (i < count - 1) {
        await new Promise(resolve => setTimeout(resolve, 200));
      }
    }
    
    const totalTime = Date.now() - startTime;
    const processedCount = results.length;
    
    console.log(`🏁 Simple batch processing complete:`);
    console.log(`   📊 Processed: ${processedCount}`);
    console.log(`   ✅ Successful: ${successCount}`);
    console.log(`   ⏭️ Skipped: ${skipCount}`);
    console.log(`   ❌ Errors: ${errorCount}`);
    console.log(`   ⏱️ Time: ${totalTime}ms`);
    
    // Determine if we should continue
    const shouldContinue = processedCount === count && 
                          !results.some(r => r.status === 'end_of_data');
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        batch_summary: {
          start_index: start_index,
          requested_count: count,
          processed_count: processedCount,
          successful_count: successCount,
          skipped_count: skipCount,
          error_count: errorCount,
          processing_time_ms: totalTime,
          
          next_batch: shouldContinue ? {
            start_index: start_index + processedCount,
            suggested_count: count
          } : null
        },
        
        results: results,
        
        next_steps: shouldContinue ? [
          'Continue with next batch',
          `Run with start_index: ${start_index + processedCount}, count: ${count}`,
          'Repeat until you reach end_of_data status'
        ] : [
          'Batch processing complete or end reached',
          results.some(r => r.status === 'end_of_data') 
            ? 'No more conversions to process'
            : 'Check results for any errors'
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Simple batch processor failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Simple batch processor failed', 
        message: error.message 
      })
    };
  }
};
