const handler = async (event, context) => {
  if (event.httpMethod !== 'POST') {
    return {
      statusCode: 405,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: 'Method not allowed'
    };
  }

  try {
    const data = JSON.parse(event.body);
    
    // Extract all the tracking data
    const trackingData = {
      timestamp: new Date().toISOString(),
      source: data.source || 'direct',
      campaign: data.campaign || 'none',
      content: data.content || 'none',
      page_url: data.page_url || '',
      email: data.email || '',
      name: data.name || '',
      phone: data.phone || '',
      ip_address: event.headers['x-forwarded-for'] || event.headers['client-ip'] || '',
      user_agent: event.headers['user-agent'] || ''
    };
    
    console.log('Conversion tracked:', trackingData);
    
    // TODO: Here we'll add CSV storage next
    
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
