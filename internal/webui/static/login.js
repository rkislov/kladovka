const form = document.getElementById('login-form');
form.onsubmit = async event => {
  event.preventDefault();
  const button = document.getElementById('login-submit'), error = document.getElementById('login-error');
  button.disabled = true; button.textContent = 'Входим…'; error.hidden = true;
  try {
    const response = await fetch('/api/session', {
      method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ user: form.elements.username.value, password: form.elements.password.value }),
    });
    if (!response.ok) throw Error(response.status === 401 ? 'Проверьте имя и пароль.' : response.status === 429 ? 'Слишком много попыток. Подождите минуту.' : 'Не удалось войти. Повторите попытку.');
    form.elements.password.value = '';
    location.replace('/ui/');
  } catch (e) { error.textContent = e.message; error.hidden = false; }
  finally { button.disabled = false; button.textContent = 'Войти'; }
};
