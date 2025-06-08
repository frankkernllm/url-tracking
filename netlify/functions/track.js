// File: netlify/functions/track.js
// Redis-powered conversion tracking

const handler = async (event, context) => {
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
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
      
      const userIP = event.headers['x-forwarded-for']?.split(',')[0]?.trim() || 
                     event.headers['x-real-ip'] || 
                     'unknown';
      
      try {
        const lookupResponse = await fetch(
          `https://trackingojoy.netlify.app/.netlify/functions/store-attribution?ip=${userIP}&timestamp=${new Date().toISOString()}`,
          { method: 'GET' }
        );
        
        if (lookupResponse.ok) {
          const lookupResult = await lookupResponse.json();
          if (lookupResult.found) {
            attributionData = lookupResult.data;
            console.log('✅ Attribution data found via lookup');
          } else {
            console.log('⚠️ No attribution data found for this user');
          }
        }
      } catch (lookupError) {
        console.log('❌ Attribution lookup failed:', lookupError.message);
      }
    }
    
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
      landing_page: attributionData?.landing_page || '',
      
      page_url: data.page_url || attributionData?.landing_page || '',
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
      
      ip_address: event.headers['x-forwarded-for'] || '',
      user_agent: attributionData?.user_agent || event.headers['user-agent'] || '',
      
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
