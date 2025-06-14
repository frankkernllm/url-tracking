// netlify/functions/attribution-recovery-3phase.js
// Alternative version that uses the analytics endpoint instead of direct Redis access

exports.handler = async (event, context) => {
    // CORS headers
    const headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Content-Type': 'application/json'
    };

    if (event.httpMethod === 'OPTIONS') {
        return { statusCode: 200, headers, body: '' };
    }

    // API Key validation using environment variable
    const apiKey = event.headers['x-api-key'];
    const expectedApiKey = process.env.OJOY_API_KEY;
    if (apiKey !== expectedApiKey) {
        return {
            statusCode: 401,
            headers,
            body: JSON.stringify({ error: 'Invalid API key' })
        };
    }

    try {
        console.log('🎯 Starting Three-Phase Attribution Recovery (Analytics-Based)');
        
        // Step 1: Get unattributed conversions from analytics API
        const unattributedConversions = await getUnattributedConversionsFromAnalytics();
        
        if (unattributedConversions.length === 0) {
            return {
                statusCode: 200,
                headers,
                body: JSON.stringify({
                    success: true,
                    message: 'No unattributed conversions found',
                    results: { total: 0, recovered: 0, phases: {} }
                })
            };
        }

        console.log(`📊 Found ${unattributedConversions.length} unattributed conversions from analytics`);

        // Step 2: Run three-phase recovery
        const recoveryResults = await runThreePhaseRecovery(unattributedConversions);

        // Step 3: Update Redis with recovered attributions (if we found any)
        if (recoveryResults.matches.length > 0) {
            await updateRecoveredAttributions(recoveryResults.matches);
        }

        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({
                success: true,
                results: recoveryResults,
                message: `Recovery complete: ${recoveryResults.recovered}/${recoveryResults.total} conversions recovered`
            })
        };

    } catch (error) {
        console.error('❌ Recovery error:', error);
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify({
                error: 'Recovery failed',
                details: error.message
            })
        };
