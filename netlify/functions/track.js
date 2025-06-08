// File: netlify/functions/track.js
// Fixed Redis-powered conversion tracking with API key authentication

const handler = async (event, context) => {
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
        'Access-Control-Allow-Methods': 'POST, OPTIONS'
      }
    };
  }

  if (event.httpMethod !== 'POST') {
    return {
      statusCode: 405,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: 'Method not allowed'
    };
  }

  // 🔒 API Key validation for external calls (optional but recommended)
  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  const validApiKey = process.env.OJOY_API_KEY;

  // Allow internal webhook calls without API key, but require it for external calls
  const isInternalCall = !apiKey; // Webhook calls typically don't have API key
  const isValidExternalCall = apiKey && apiKey === validApiKey;

  if (!isInternalCall && !isValidExternalCall) {
    console.log('🚫 Unauthorized external access attempt');
    return {
      statusCode: 401,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ error: 'Unauthorized' })
    };
  }

  console.log('✅ Track function access authorized');

  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;

  const redis = async (command) => {
    const response = await fetch(`${redisUrl}/${command}`, {
      headers: { Authorization: `Bearer ${redisToken}` }
    });
    return response.json();
  };

  try {
    const data = JSON.parse(event.body);
    
    console.log('📥 Raw webhook/conversion data:', JSON.stringify(data, null, 2));
    
    const isSpiffyWebhook = data.email && (data.order_total || data.order_id || data.name_first);
    
    let attributionData = null;
    
    if (isSpiffyWebhook) {
      console.log('🛒 Spiffy webhook detected, looking up attribution...');
      
      // Extract customer IP from webhook payload (not server headers)
      const customerIP = data.checkoutview?.pageviewcheckout?.pageview?.ip || 
                        data.pageview?.ip || 
                        data.ip ||
                        event.headers['x-forwarded-for']?.split(',')[0]?.trim() || 
                        event.headers['x-real-ip'] || 
                        'unknown';
      
      console.log('🔍 Customer IP extracted:', customerIP);
      
      try {
        // Try direct IP-based attribution lookup
        const lookupResponse = await fetch(
          `https://trackingojoy.netlify.app/.netlify/functions/store-attribution?ip=${customerIP}&timestamp=${new Date().toISOString()}`,
          { 
            method: 'GET',
            headers: {
              'X-API-Key': process.env.OJOY_API_KEY
            }
          }
        );
        
        if (lookupResponse.ok) {
          const lookupResult = await lookupResponse.json();
          if (lookupResult.found) {
            attributionData = lookupResult.data;
            console.log('✅ Attribution data found via IP lookup');
          } else {
            console.log('⚠️ No attribution data found for this IP');
          }
        } else {
          console.log('❌ Attribution lookup failed with status:', lookupResponse.status);
        }
      } catch (lookupError) {
        console.log('❌ Attribution lookup failed:', lookupError.message);
      }

      // 🔧 ENHANCED: Try alternative attribution lookup methods if IP lookup fails
      if (!attributionData && data.email) {
        console.log('🔍 Trying email-based attribution lookup...');
        try {
          // Look for recent attribution data by email or session
          const emailKeys = await redis('keys/attribution:*');
          if (emailKeys.result && emailKeys.result.length > 0) {
            
            console.log(`🔍 Searching ${emailKeys.result.length} attribution keys for ${data.email}`);
            
            // Check recent attribution data for a match
            const recentKeys = emailKeys.result.slice(-20); // Check last 20 records
            let bestMatch = null;
            let bestMatchScore = 0;
            
            for (const key of recentKeys) {
              try {
                const attrData = await redis(`get/${key}`);
                if (attrData.result) {
                  const parsedData = JSON.parse(attrData.result);
                  
                  // Calculate time difference
                  const timeDiff = Date.now() - new Date(parsedData.timestamp).getTime();
                  
                  // Only consider attribution within 1 hour (strict timing)
                  if (timeDiff < 1 * 60 * 60 * 1000) {
                    let matchScore = 0;
                    
                    // Scoring system for attribution matching
                    // High priority: exact email match
                    if (parsedData.email && parsedData.email === data.email) {
                      matchScore += 100;
                    }
                    
                    // High priority: exact IP match
                    if (parsedData.ip_address && parsedData.ip_address === customerIP) {
                      matchScore += 100;
                    }
                    
                    // Medium priority: same source (if available in webhook)
                    if (parsedData.source && data.utm_source && parsedData.source === data.utm_source) {
                      matchScore += 50;
                    }
                    
                    // Low priority: time proximity (newer = better score)
                    const timeScore = Math.max(0, 10 - (timeDiff / (10 * 60 * 1000))); // 0-10 based on minutes
                    matchScore += timeScore;
                    
                    console.log(`🔍 Attribution match candidate:`, {
                      key: key.substring(0, 50) + '...',
                      email: parsedData.email,
                      ip: parsedData.ip_address,
                      source: parsedData.source,
                      timeDiff: Math.round(timeDiff / 60000) + 'm',
                      score: matchScore
                    });
                    
                    // Keep track of best match
                    if (matchScore > bestMatchScore) {
                      bestMatch = parsedData;
                      bestMatchScore = matchScore;
                    }
                  }
                }
              } catch (parseError) {
                // Skip invalid records
                continue;
              }
            }
            
            // Only use attribution if we found a high-confidence match
            if (bestMatch && bestMatchScore >= 100) { // Require email or IP match
              attributionData = bestMatch;
              console.log(`✅ Attribution data found via high-confidence match (score: ${bestMatchScore})`);
            } else if (bestMatch && bestMatchScore >= 50) {
              attributionData = bestMatch;
              console.log(`⚠️ Attribution data found via medium-confidence match (score: ${bestMatchScore})`);
            } else if (bestMatch && bestMatchScore >= 10) { // Accept timing-based matches within 10 minutes
              attributionData = bestMatch;
              console.log(`⚠️ Attribution data found via timing match (score: ${bestMatchScore}) - low confidence`);
            } else {
              console.log(`❌ No high-confidence attribution match found (best score: ${bestMatchScore || 0})`);
            }
          }
        } catch (emailLookupError) {
          console.log('❌ Email-based attribution lookup failed:', emailLookupError.message);
        }
      }
    }
    
    // Extract customer data from Spiffy webhook payload
    const customerUserAgent = data.checkoutview?.pageviewcheckout?.pageview?.user_agent || 
                             data.pageview?.user_agent || 
                             data.user_agent ||
                             attributionData?.user_agent || 
                             event.headers['user-agent'] || '';
    
    const customerIPAddress = data.checkoutview?.pageviewcheckout?.pageview?.ip || 
                             data.pageview?.ip || 
                             data.ip ||
                             event.headers['x-forwarded-for'] || '';
    
    // Extract landing page from webhook or attribution
    const landingPage = data.checkoutview?.pageviewcheckout?.pageview?.url ||
                       attributionData?.landing_page ||
                       data.page_url || '';
    
    const trackingData = {
      timestamp: new Date().toISOString(),
      event_type: isSpiffyWebhook ? 'purchase' : 'conversion',
      
      source: attributionData?.source || 
              data.utm_source || data.source || 
              'direct',
      campaign: attributionData?.utm_campaign || 
                data.utm_campaign || data.campaign || 
                'none',
      content: attributionData?.utm_content || 
               data.utm_content || data.content || 
               'none',
      medium: attributionData?.utm_medium || 
              data.utm_medium || 
              'none',
      
      source_type: attributionData?.source_type || 'unknown',
      referrer_url: attributionData?.referrer_url || data.page_url || '',
      landing_page: landingPage,
      
      page_url: data.page_url || landingPage || '',
      conversion_page: data.conversion_page || '',
      
      email: data.email || '',
      name: data.name || data.name_first || '',
      phone: data.phone || data.phone_number || '',
      
      ...(isSpiffyWebhook && {
        order_id: data.order_id,
        order_total: data.order_total,
        currency: data.currency,
        offer_name: data.offer_name,
        payment_gateway: data.payment_gateway,
        subscription_id: data.subscription_id
      }),
      
      // Use customer data from webhook payload, not server headers
      ip_address: customerIPAddress,
      user_agent: customerUserAgent,
      
      ...(attributionData && {
        screen_resolution: attributionData.screen_resolution,
        timezone: attributionData.timezone,
        language: attributionData.language,
        is_returning_visitor: attributionData.is_returning_visitor
      }),
      
      attribution_found: !!attributionData,
      attribution_source: attributionData ? 'lookup' : 'direct'
    };
    
    console.log('📊 Final tracking data:', JSON.stringify(trackingData, null, 2));
    
    // Store directly to Redis instead of calling analytics endpoint
    try {
      if (isSpiffyWebhook) {
        const key = `conversions:${trackingData.timestamp}:${Math.random()}`;
        await redis(`set/${key}/${encodeURIComponent(JSON.stringify(trackingData))}`);
        console.log('📊 Conversion stored for analytics');
      } else {
        const key = `pageviews:${trackingData.timestamp}:${Math.random()}`;
        const pageViewData = { ...trackingData, event_type: 'page_view' };
        await redis(`set/${key}/${encodeURIComponent(JSON.stringify(pageViewData))}`);
        console.log('📊 Page view stored for analytics');
      }
    } catch (storageError) {
      console.log('⚠️ Redis storage failed:', storageError.message);
    }
    
    return {
      statusCode: 200,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ 
        success: true, 
        message: 'Conversion tracked successfully',
        data: trackingData,
        attribution_found: !!attributionData
      })
    };
  } catch (error) {
    console.error('❌ Tracking error:', error);
    return {
      statusCode: 500,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ error: error.message })
    };
  }
};

module.exports = { handler };
