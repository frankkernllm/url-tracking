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
    
    const trackingData = {
      timestamp: new Date().toISOString(),
      source: data.source || 'direct',
      campaign: data.campaign || 'none',
      content: data.content || 'none',
      page_url: data.page_url || '',
      conversion_page: data.conversion_page || '',
      email: data.email || '',
      name: data.name || '',
      phone: data.phone || '',
      ip_address: event.headers['x-forwarded-for'] || '',
      user_agent: event.headers['user-agent'] || ''
    };
    
    console.log('Conversion tracked:', trackingData);
    
    return {
      statusCode: 200,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ 
        success: true, 
        message: 'Conversion tracked successfully',
        data: trackingData 
      })
    };
  } catch (error) {
    return {
      statusCode: 500,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ error: error.message })
    };
  }
};

module.exports = { handler };
