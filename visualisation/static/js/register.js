async function handleRegister(event) {
    event.preventDefault();
    
    const username = document.getElementById('username').value;
    const password = document.getElementById('password').value;
    const confirmPassword = document.getElementById('confirmPassword').value;

    if (password !== confirmPassword) {
        alert('两次输入的密码不一致！');
        return;
    }

    try {
        const response = await fetch('/api/register', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ username, password })
        });

        const data = await response.json();
        console.log(data);
        
        if (response.ok) {
            alert('注册成功！');
            window.location.href = '/';
        } else {
            alert(data.error || '注册失败');
        }
    } catch (error) {
        alert('服务器连接失败');
    }
}

document.getElementById('registerForm').addEventListener('submit', handleRegister);