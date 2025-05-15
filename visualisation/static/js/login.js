async function handleLogin(event) {
    event.preventDefault();
    
    const username = document.getElementById('username').value;
    const password = document.getElementById('password').value;

    try {
        const response = await fetch('/api/login', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ username, password })
        });

        const data = await response.json();

        if (response.ok) {
            localStorage.setItem('isLoggedIn', 'true');
            localStorage.setItem('username', username);
            localStorage.setItem('token', data.token);  // 保存token
            window.location.href = '/dashboard';
        } else {
            alert(data.error || '登录失败');
        }
    } catch (error) {
        alert('服务器连接失败');
    }
}

document.getElementById('loginForm').addEventListener('submit', handleLogin);