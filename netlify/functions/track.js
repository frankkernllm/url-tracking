// File: netlify/functions/track.js
// Enhanced Netlify Function: track.js
// Handles both conversions and attribution lookup

const handler = async (event, context) => {
  // Handle CORS preflight
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

  try {
    const data = JSON.parse(event.body);
    
    // 🔍 LOG THE FULL RAW DATA
    console.log('📥 Raw webhook/conversion data:', JSON.stringify(data, null, 2));
    
    // Determine if this is a Spiffy webhook (has email, order info)
    const isSpiffyWebhook = data.email && (data.order_total || data.order_id || data.name_first);
    
    let attributionData = null;
    
    // If it's a Spiffy webhook, try to lookup attribution data
    if (isSpiffyWebhook) {
      console.log('🛒 Spiffy webhook detected, looking up attribution...');
      
      // Get user's IP from the original request
      const userIP = event.headers['x-forwarded-for']?.split(',')[0]?.trim() || 
                     event.headers['x-real-ip'] || 
                     'unknown';
      
      try {
        // Try to lookup attribution data
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
    
    // Build comprehensive tracking data
    const trackingData = {
      timestamp: new Date().toISOString(),
      event_type: isSpiffyWebhook ? 'purchase' : 'conversion',
      
      // Attribution data (from lookup or direct)
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
      
      // Traffic source analysis
      source_type: attributionData?.source_type || 'unknown',
      referrer_url: attributionData?.referrer_url || data.page_url || '',
      landing_page: attributionData?.landing_page || '',
      
      // Conversion/purchase data
      page_url: data.page_url || attributionData?.landing_page || '',
      conversion_page: data.conversion_page || '',
      
      // Customer data
      email: data.email || '',
      name: data.name || data.name_first || '',
      phone: data.phone || data.phone_number || '',
      
      // Spiffy-specific data
      ...(isSpiffyWebhook && {
        order_id: data.order_id,
        order_total: data.order_total,
        currency: data.currency,
        offer_name: data.offer_name,
        payment_gateway: data.payment_gateway,
        subscription_id: data.subscription_id
      }),
      
      // Technical data
      ip_address: event.headers['x-forwarded-for'] || '',
      user_agent: attributionData?.user_agent || event.headers['user-agent'] || '',
      
      // Device info (if available from attribution)
      ...(attributionData && {
        screen_resolution: attributionData.screen_resolution,
        timezone: attributionData.timezone,
        language: attributionData.language,
        is_returning_visitor: attributionData.is_returning_visitor
      }),
      
      // Attribution lookup info
      attribution_found: !!attributionData,
      attribution_source: attributionData ? 'lookup' : 'direct'
    };
    
    console.log('📊 Final tracking data:', JSON.stringify(trackingData, null, 2));
    
    // Store conversion data for analytics (if it's a purchase)
    if (isSpiffyWebhook) {
      try {
        await fetch('https://trackingojoy.netlify.app/.netlify/functions/analytics', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(trackingData)
        });
        console.log('📊 Conversion stored for analytics');
      } catch (analyticsError) {
        console.log('⚠️ Analytics storage failed:', analyticsError.message);
      }
    } else {
      // This is a page view or other event, also store for analytics
      try {
        await fetch('https://trackingojoy.netlify.app/.netlify/functions/analytics', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            ...trackingData,
            event_type: 'page_view'
          })
        });
        console.log('📊 Page view stored for analytics');
      } catch (analyticsError) {
        console.log('⚠️ Page view analytics storage failed:', analyticsError.message);
      }
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
