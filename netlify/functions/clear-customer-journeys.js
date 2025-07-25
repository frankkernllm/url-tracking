// netlify/functions/clear-customer-journeys.js
// Clears all existing customer journey records for rebuilding

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
    console.log('🗑️ CLEARING ALL CUSTOMER JOURNEYS: Starting cleanup process...');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max
    
    const redis = initializeRedis();
    
    // Get parameters
    const body = event.body ? JSON.parse(event.body) : {};
    const { confirm_delete = false } = body;
    
    if (!confirm_delete) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({
          error: 'Safety check required',
          message: 'To confirm deletion of ALL customer journeys, set confirm_delete: true',
          warning: 'This action cannot be undone'
        })
      };
    }
    
    console.log('🗑️ CONFIRMED: Proceeding with journey deletion...');
    
    // Step 1: Scan and collect all journey keys
    const journeyKeys = await scanAllJourneyKeys(redis, maxProcessingTime - (Date.now() - startTime));
    console.log(`🔍 Found ${journeyKeys.length} journey keys to delete`);
    
    if (journeyKeys.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          message: 'No customer journeys found to delete',
          keys_deleted: 0
        })
      };
    }
    
    // Step 2: Delete journeys in batches
    const deletionResults = await deleteJourneysInBatches(redis, journeyKeys, maxProcessingTime - (Date.now() - startTime));
    
    const totalTime = Date.now() - startTime;
    
    console.log(`✅ JOURNEY CLEANUP COMPLETE: ${deletionResults.keys_deleted} journeys deleted in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        cleanup_complete: true,
        execution_summary: {
          journey_keys_found: journeyKeys.length,
          keys_deleted: deletionResults.keys_deleted,
          keys_failed: deletionResults.keys_failed,
          processing_time_ms: totalTime,
          deletion_batches: deletionResults.batches_processed
        },
        next_steps: [
          'All customer journeys have been cleared',
          'Run build-customer-journeys.js to rebuild with fixed logic',
          'Use force_rebuild: false for normal processing'
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Journey cleanup failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Journey cleanup failed', 
        message: error.message 
      })
    };
  }
};

// Scan all journey keys
async function scanAllJourneyKeys(redis, maxTime) {
  console.log('🔍 Scanning for all customer journey keys...');
  
  const journeyKeys = [];
  let cursor = '0';
  let iterations = 0;
  const maxIterations = 100;
  const scanStartTime = Date.now();
  
  try {
    do {
      if (Date.now() - scanStartTime > maxTime - 3000) {
        console.log('⏰ Time limit during key scanning, stopping');
        break;
      }
      
      const scanResult = await redis(`scan/${cursor}/match/customer_journey:*/count/200`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      iterations++;
      
      // Add all found keys
      journeyKeys.push(...keys);
      
      if (journeyKeys.length % 1000 === 0 && journeyKeys.length > 0) {
        console.log(`🔍 Key scanning progress: ${journeyKeys.length} journey keys found`);
      }
      
    } while (cursor !== '0' && iterations < maxIterations);
    
  } catch (scanError) {
    console.log(`⚠️ Key scan error: ${scanError.message}`);
  }
  
  console.log(`✅ Key scanning complete: ${journeyKeys.length} total journey keys found`);
  return journeyKeys;
}

// Delete journeys in batches
async function deleteJourneysInBatches(redis, journeyKeys, maxTime) {
  console.log(`🗑️ Deleting ${journeyKeys.length} journey keys in batches...`);
  
  const deleteStartTime = Date.now();
  let keysDeleted = 0;
  let keysFailed = 0;
  let batchesProcessed = 0;
  const batchSize = 50; // Delete 50 keys per batch
  
  try {
    for (let i = 0; i < journeyKeys.length; i += batchSize) {
      if (Date.now() - deleteStartTime > maxTime - 2000) {
        console.log('⏰ Time limit during deletion, stopping');
        break;
      }
      
      const batch = journeyKeys.slice(i, i + batchSize);
      batchesProcessed++;
      
      // Delete batch in parallel
      const batchPromises = batch.map(async (key) => {
        try {
          await redis(`del/${key}`, 2000); // 2 second timeout per deletion
          keysDeleted++;
          return true;
        } catch (deleteError) {
          console.warn(`⚠️ Failed to delete key ${key}: ${deleteError.message}`);
          keysFailed++;
          return false;
        }
      });
      
      await Promise.all(batchPromises);
      
      if (batchesProcessed % 10 === 0) {
        console.log(`🗑️ Deletion progress: ${keysDeleted} deleted, ${keysFailed} failed (batch ${batchesProcessed})`);
      }
    }
    
  } catch (batchError) {
    console.log(`⚠️ Batch deletion error: ${batchError.message}`);
  }
  
  console.log(`✅ Deletion complete: ${keysDeleted} deleted, ${keysFailed} failed in ${batchesProcessed} batches`);
  
  return {
    keys_deleted: keysDeleted,
    keys_failed: keysFailed,
    batches_processed: batchesProcessed
  };
}

// Initialize Redis helper
function initializeRedis() {
  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
  
  return async (command, timeoutMs = 5000) => {
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
