document.addEventListener('DOMContentLoaded', () => {
    const chatMessages = document.getElementById('chat-messages');
    const queryInput = document.getElementById('query-input');
    const sendButton = document.getElementById('send-button');
    
    // Generate a unique client ID for WebSocket connection
    const clientId = 'client_' + Math.random().toString(36).substr(2, 9);
    
    // Connect to WebSocket
    const ws = new WebSocket(`ws://${window.location.host}/ws/${clientId}`);
    
    ws.onopen = () => {
        console.log('WebSocket connected');
        addMessage('Connected to Federal Registry Assistant. How can I help you?', 'assistant');
    };
    
    ws.onmessage = (event) => {
        const response = JSON.parse(event.data);
        if (response.error) {
            addMessage(response.error, 'error');
        } else {
            addMessage(response.answer, 'assistant');
        }
    };
    
    ws.onerror = (error) => {
        console.error('WebSocket error:', error);
        addMessage('Connection error. Please try again.', 'error');
    };
    
    ws.onclose = () => {
        console.log('WebSocket disconnected');
        addMessage('Connection lost. Please refresh the page.', 'error');
    };
    
    function addMessage(text, type) {
        const messageDiv = document.createElement('div');
        messageDiv.className = `message ${type}-message`;
        messageDiv.textContent = text;
        chatMessages.appendChild(messageDiv);
        chatMessages.scrollTop = chatMessages.scrollHeight;
    }
    
    async function sendQuery() {
        const query = queryInput.value.trim();
        if (!query) return;
        
        // Add user message to chat
        addMessage(query, 'user');
        
        // Clear input
        queryInput.value = '';
        
        // Disable input while processing
        queryInput.disabled = true;
        sendButton.disabled = true;
        
        try {
            // Send message through WebSocket
            ws.send(query);
        } catch (error) {
            console.error('Error sending message:', error);
            addMessage('Error sending message. Please try again.', 'error');
        } finally {
            // Re-enable input
            queryInput.disabled = false;
            sendButton.disabled = false;
            queryInput.focus();
        }
    }
    
    // Event listeners
    sendButton.addEventListener('click', sendQuery);
    
    queryInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            sendQuery();
        }
    });
    
    // Focus input on load
    queryInput.focus();
});