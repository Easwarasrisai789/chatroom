"""
Non-blocking Combined One-to-One TCP Chat (Server + Client)
- Server and Client pages in one Tkinter app (navbar)
- Server accepts ONE client at a time
- All blocking socket operations run in background threads
- UI remains responsive (no freezing)
- Use this on LAN: open app twice (one as server, one as client) or run on two machines
"""

import socket
import threading
import queue
import time
from tkinter import *
from tkinter import scrolledtext, messagebox

PORT = 8080
ENC = "utf-8"
HOST_BIND = ""  # bind to all interfaces

# ----------------------
# Server backend (one client) - non-blocking
# ----------------------
class NonBlockingServer:
    def __init__(self, ui_log=None, incoming=None):
        self.sock = None
        self.conn = None
        self.addr = None
        self.client_name = None
        self.running = False
        self.ui_log = ui_log            # function(str) -> queue put
        self.incoming_cb = incoming    # function(dict) -> queue put
        self._accept_thread = None

    def log(self, text):
        if self.ui_log:
            self.ui_log(text)
        else:
            print("[SERVER]", text)

    def start(self, host=HOST_BIND, port=PORT):
        if self.running:
            self.log("Server already running.")
            return
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # bind to host/port
            self.sock.bind((host, port))
            self.sock.listen(1)
            self.running = True
            # start accept loop in background thread
            self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
            self._accept_thread.start()
            self.log(f"Server listening on {self._bound_ip()}:{port}")
        except Exception as e:
            self.log(f"Start error: {e}")
            raise

    def _bound_ip(self):
        try:
            import socket as _s
            return _s.gethostbyname(_s.gethostname())
        except:
            return "127.0.0.1"

    def _accept_loop(self):
        # runs in background thread
        while self.running:
            try:
                conn, addr = self.sock.accept()  # blocking but inside thread
                # if already connected, politely refuse
                if self.conn:
                    try:
                        conn.sendall("BUSY".encode(ENC))
                        conn.close()
                    except:
                        pass
                    continue
                self.conn = conn
                self.addr = addr
                # start handler thread for this client
                handler = threading.Thread(target=self._handle_client, args=(conn, addr), daemon=True)
                handler.start()
            except Exception as e:
                if self.running:
                    self.log(f"Accept loop error: {e}")
                break

    def _handle_client(self, conn, addr):
        try:
            # receive initial name handshake (blocking in handler thread only)
            conn.settimeout(8.0)
            raw = conn.recv(4096)
            conn.settimeout(None)
            if not raw:
                self._cleanup_conn()
                return
            name = raw.decode(ENC)
            self.client_name = name
            # notify GUI about connection
            if self.incoming_cb:
                self.incoming_cb({'type': 'system', 'msg': f"Client '{name}' connected from {addr}"})
            # now receive messages continuously (still in handler thread)
            while self.running and self.conn:
                try:
                    data = conn.recv(4096)
                    if not data:
                        break
                    txt = data.decode(ENC)
                    if self.incoming_cb:
                        self.incoming_cb({'type': 'msg', 'from': self.client_name or "Client", 'msg': txt})
                except Exception:
                    break
        except Exception as e:
            self.log(f"Client handler error: {e}")
        finally:
            if self.incoming_cb:
                self.incoming_cb({'type': 'system', 'msg': f"Client '{self.client_name}' disconnected."})
            self._cleanup_conn()

    def send(self, text):
        if not self.conn:
            self.log("No client connected.")
            return False
        try:
            self.conn.sendall(text.encode(ENC))
            return True
        except Exception as e:
            self.log(f"Send error: {e}")
            return False

    def send_servername(self, name):
        if not self.conn:
            return
        try:
            # send server display name to client (handshake)
            self.conn.sendall(name.encode(ENC))
        except:
            pass

    def stop(self):
        self.running = False
        try:
            if self.conn:
                self.conn.close()
        except:
            pass
        try:
            if self.sock:
                self.sock.close()
        except:
            pass
        self.conn = None
        self.addr = None
        self.client_name = None
        self.log("Server stopped.")

    def _cleanup_conn(self):
        try:
            if self.conn:
                self.conn.close()
        except:
            pass
        self.conn = None
        self.addr = None
        self.client_name = None

# ----------------------
# Client backend - non-blocking connect
# ----------------------
class NonBlockingClient:
    def __init__(self, incoming=None, system=None):
        self.sock = None
        self.name = None
        self.server_name = None
        self.running = False
        self.incoming_cb = incoming   # function(dict) -> queue
        self.system_cb = system       # function(str) -> queue
        self._recv_thread = None

    def connect_async(self, server_ip, port, name):
        # start connect in background thread and return immediately
        thread = threading.Thread(target=self._connect_thread, args=(server_ip, port, name), daemon=True)
        thread.start()

    def _connect_thread(self, server_ip, port, name):
        # real connect happens here (background)
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(6.0)  # short timeout to avoid long block
            sock.connect((server_ip, port))
            sock.settimeout(None)
            # send name handshake
            sock.sendall(name.encode(ENC))
            # wait for server reply (server name or BUSY)
            sock.settimeout(6.0)
            raw = sock.recv(4096)
            sock.settimeout(None)
            if not raw:
                raise Exception("No response from server")
            resp = raw.decode(ENC)
            if resp == "BUSY":
                try:
                    sock.close()
                except:
                    pass
                if self.system_cb:
                    self.system_cb("Server busy")
                return
            # connected successfully
            self.sock = sock
            self.name = name
            self.server_name = resp
            self.running = True
            if self.system_cb:
                self.system_cb(f"Connected to {self.server_name}")
            # start receiving in background
            self._recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
            self._recv_thread.start()
        except Exception as e:
            try:
                if sock:
                    sock.close()
            except:
                pass
            if self.system_cb:
                self.system_cb(f"Connect error: {e}")

    def _recv_loop(self):
        try:
            while self.running:
                raw = self.sock.recv(4096)
                if not raw:
                    break
                txt = raw.decode(ENC)
                if self.incoming_cb:
                    self.incoming_cb({'type': 'msg', 'from': self.server_name or "Server", 'msg': txt})
        except Exception as e:
            if self.system_cb:
                self.system_cb(f"Receive error: {e}")
        finally:
            self.disconnect_local()
            if self.system_cb:
                self.system_cb("Disconnected from server")

    def send(self, text):
        if not self.sock:
            return False
        try:
            self.sock.sendall(text.encode(ENC))
            return True
        except Exception:
            return False

    def disconnect_local(self):
        self.running = False
        try:
            if self.sock:
                self.sock.close()
        except:
            pass
        self.sock = None

# ----------------------
# GUI App
# ----------------------
class App:
    def __init__(self, root):
        self.root = root
        root.title("Non-blocking One-to-One TCP Chat")
        root.geometry("760x640")

        # backends with callbacks
        self.server = NonBlockingServer(ui_log=self.server_log_queue, incoming=self.server_incoming_queue)
        self.client = NonBlockingClient(incoming=self.client_incoming_queue, system=self.client_system_queue)

        # queues for safe GUI updates
        self.server_q = queue.Queue()
        self.client_q = queue.Queue()

        # navbar
        nav = Frame(root, bg="#222", height=44)
        nav.pack(side=TOP, fill=X)
        Button(nav, text="Server", command=self.show_server_page, bg="#444", fg="white").pack(side=LEFT, padx=8, pady=6)
        Button(nav, text="Client", command=self.show_client_page, bg="#444", fg="white").pack(side=LEFT, padx=8, pady=6)

        # pages
        self.server_frame = Frame(root)
        self.client_frame = Frame(root)

        self._build_server_page()
        self._build_client_page()

        self.show_server_page()

        # start polling queues regularly
        self._poll_queues()

    # -------------
    # Server page
    # -------------
    def _build_server_page(self):
        f = self.server_frame
        Label(f, text="SERVER", font=("Arial", 16, "bold")).pack(pady=6)

        top = Frame(f)
        top.pack(fill=X, padx=8)
        self.server_ip_label = Label(top, text="Server IP: (Start server to show IP)")
        self.server_ip_label.pack(side=LEFT)
        Button(top, text="Start Server", command=self.on_start_server, bg="#2ecc71").pack(side=RIGHT, padx=6)
        Button(top, text="Stop Server", command=self.on_stop_server, bg="#e74c3c").pack(side=RIGHT, padx=6)

        Label(f, text="Server Display Name:").pack(anchor=W, padx=8, pady=(6,0))
        self.server_name_entry = Entry(f, width=30)
        self.server_name_entry.pack(anchor=W, padx=8)

        Label(f, text="Chat Log:").pack(anchor=W, padx=8, pady=(8,0))
        self.server_chat = scrolledtext.ScrolledText(f, height=18)
        self.server_chat.pack(fill=BOTH, expand=False, padx=8)

        send_frame = Frame(f)
        send_frame.pack(fill=X, padx=8, pady=8)
        Label(send_frame, text="Message:").pack(side=LEFT)
        self.server_msg_entry = Entry(send_frame, width=50)
        self.server_msg_entry.pack(side=LEFT, padx=6)
        Button(send_frame, text="Send", command=self.on_server_send, bg="#3498db").pack(side=LEFT, padx=6)

        self.server_status_label = Label(f, text="Status: Stopped")
        self.server_status_label.pack(anchor=W, padx=8, pady=(0,8))

    def on_start_server(self):
        try:
            self.server.start(port=PORT)
            ip = self.server._bound_ip()
            self.server_ip_label.config(text=f"Server IP: {ip}  Port: {PORT}")
            self.server_status_label.config(text="Status: Listening (waiting for client)")
            self.server_log_queue("Server started.")
        except Exception as e:
            messagebox.showerror("Start Server Error", str(e))

    def on_stop_server(self):
        self.server.stop()
        self.server_log_queue("Server stopped.")
        self.server_ip_label.config(text="Server IP: (Start server to show IP)")
        self.server_status_label.config(text="Status: Stopped")

    def server_log_queue(self, text):
        self.server_q.put({'type': 'log', 'msg': text})

    def server_incoming_queue(self, obj):
        # obj is {'type':..., 'msg'...} or {'type':'msg','from':..,'msg':..}
        self.server_q.put({'type': 'incoming', 'obj': obj})

    def on_server_send(self):
        text = self.server_msg_entry.get().strip()
        if not text:
            return
        if not self.server.conn:
            self.server_log_queue("No client connected to send.")
            return
        ok = self.server.send(text)
        if ok:
            self.server_log_queue(f"Me: {text}")
        else:
            self.server_log_queue("Send failed.")
        self.server_msg_entry.delete(0, END)

    # -------------
    # Client page
    # -------------
    def _build_client_page(self):
        f = self.client_frame
        Label(f, text="CLIENT", font=("Arial", 16, "bold")).pack(pady=6)

        top = Frame(f)
        top.pack(fill=X, padx=8)
        Label(top, text="Server IP:").pack(side=LEFT)
        self.client_ip_entry = Entry(top, width=20)
        self.client_ip_entry.pack(side=LEFT, padx=6)
        Label(top, text="Your Name:").pack(side=LEFT, padx=6)
        self.client_name_entry = Entry(top, width=14)
        self.client_name_entry.pack(side=LEFT, padx=6)
        Button(top, text="Connect", command=self.on_client_connect, bg="#2ecc71").pack(side=LEFT, padx=6)
        Button(top, text="Disconnect", command=self.on_client_disconnect, bg="#e74c3c").pack(side=LEFT, padx=6)

        Label(f, text="Chat Log:").pack(anchor=W, padx=8, pady=(8,0))
        self.client_chat = scrolledtext.ScrolledText(f, height=18)
        self.client_chat.pack(fill=BOTH, expand=False, padx=8)

        send_frame = Frame(f)
        send_frame.pack(fill=X, padx=8, pady=8)
        Label(send_frame, text="Message:").pack(side=LEFT)
        self.client_msg_entry = Entry(send_frame, width=50)
        self.client_msg_entry.pack(side=LEFT, padx=6)
        Button(send_frame, text="Send", command=self.on_client_send, bg="#3498db").pack(side=LEFT, padx=6)

        self.client_status_label = Label(f, text="Status: Disconnected")
        self.client_status_label.pack(anchor=W, padx=8, pady=(0,8))

    def on_client_connect(self):
        ip = self.client_ip_entry.get().strip()
        name = self.client_name_entry.get().strip()
        if not ip or not name:
            messagebox.showwarning("Missing", "Please enter Server IP and your name.")
            return
        # start connect in background (non-blocking)
        self.client.connect_async(ip, PORT, name)
        self.client_status_label.config(text="Status: Connecting...")

    def on_client_disconnect(self):
        self.client.disconnect_local()
        self.client_q.put({'type': 'system', 'msg': "Disconnected"})
        self.client_status_label.config(text="Status: Disconnected")

    def on_client_send(self):
        txt = self.client_msg_entry.get().strip()
        if not txt:
            return
        ok = self.client.send(txt)
        if ok:
            self.client_q.put({'type': 'chat', 'msg': f"Me: {txt}"})
        else:
            self.client_q.put({'type': 'system', 'msg': "Send failed. Are you connected?"})
        self.client_msg_entry.delete(0, END)

    # -------------
    # Page switching
    # -------------
    def show_server_page(self):
        self.client_frame.pack_forget()
        self.server_frame.pack(fill=BOTH, expand=True)

    def show_client_page(self):
        self.server_frame.pack_forget()
        self.client_frame.pack(fill=BOTH, expand=True)

    # -------------
    # Queues polling (update GUI safely)
    # -------------
    def _poll_queues(self):
        # server queue
        try:
            while True:
                item = self.server_q.get_nowait()
                t = item.get('type')
                if t == 'log':
                    self.server_chat.insert(END, item.get('msg') + "\n")
                    self.server_chat.see(END)
                elif t == 'incoming':
                    obj = item.get('obj')
                    if obj.get('type') == 'system':
                        self.server_chat.insert(END, f"[SYSTEM] {obj.get('msg')}\n")
                        # if client just connected, send server display name automatically
                        if "connected" in obj.get('msg', '').lower():
                            svname = self.server_name_entry.get().strip() or "Server"
                            # send handshake name back to client (in background thread)
                            try:
                                threading.Thread(target=self.server.send_servername, args=(svname,), daemon=True).start()
                            except:
                                pass
                            self.server_status_label.config(text=f"Status: Client connected ({self.server.client_name})")
                        elif "disconnected" in obj.get('msg', '').lower():
                            self.server_status_label.config(text="Status: Listening (waiting for client)")
                    elif obj.get('type') == 'msg':
                        fr = obj.get('from', 'Client')
                        msg = obj.get('msg', '')
                        self.server_chat.insert(END, f"{fr}: {msg}\n")
                    self.server_chat.see(END)
        except queue.Empty:
            pass

        # client queue
        try:
            while True:
                item = self.client_q.get_nowait()
                t = item.get('type')
                if t == 'chat':
                    self.client_chat.insert(END, item.get('msg') + "\n")
                    self.client_chat.see(END)
                elif t == 'system':
                    self.client_chat.insert(END, f"[SYSTEM] {item.get('msg')}\n")
                    self.client_chat.see(END)
                    # update status label accordingly
                    msg = item.get('msg', '').lower()
                    if msg.startswith("connected to"):
                        self.client_status_label.config(text=f"Status: Connected to {self.client.server_name}")
                    elif "connect error" in msg or "server busy" in msg:
                        self.client_status_label.config(text="Status: Disconnected")
                    elif msg == "disconnected" or "disconnected from server" in msg.lower():
                        self.client_status_label.config(text="Status: Disconnected")
                elif t == 'incoming':
                    obj = item.get('obj', {})
                    if obj.get('type') == 'msg':
                        fr = obj.get('from', 'Server')
                        msg = obj.get('msg', '')
                        self.client_chat.insert(END, f"{fr}: {msg}\n")
                        self.client_chat.see(END)
                    else:
                        self.client_chat.insert(END, f"[DEBUG] {obj}\n")
                        self.client_chat.see(END)
        except queue.Empty:
            pass

        # schedule next poll
        self.root.after(120, self._poll_queues)

    # -------------
    # Callbacks used by backends to queue UI updates
    # -------------
    def server_log_queue(self, text):
        self.server_q.put({'type': 'log', 'msg': text})

    def server_incoming_queue(self, obj):
        self.server_q.put({'type': 'incoming', 'obj': obj})

    def client_incoming_queue(self, obj):
        self.client_q.put({'type': 'incoming', 'obj': obj})

    def client_system_queue(self, text):
        self.client_q.put({'type': 'system', 'msg': text})

# ----------------------
# Run
# ----------------------
if __name__ == "__main__":
    root = Tk()
    app = App(root)
    root.mainloop()
