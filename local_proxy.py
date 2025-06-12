import base64
import logging
import os
import socket
import socketserver
import threading
import shutil
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Tuple, Dict, Optional, Type, Any
from urllib.parse import urlparse
import time

# Try importing python-dotenv to load .env file
try:
    from dotenv import load_dotenv
    load_dotenv()  # Load .env file if it exists
except ImportError:
    print("Warning: python-dotenv not available. Environment variables must be set manually.")

# Try importing requests, provide helpful error if missing
try:
    import requests
except ImportError:
    print("Error: The 'requests' library is required. Please install it using 'pip install requests'")
    exit(1)


# --- Configuration ---
# Configure logging format and level
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - pid:%(process)d - %(threadName)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# --- Proxy Handler ---
class UpstreamProxyRequestHandler(BaseHTTPRequestHandler):
    """
    Handles incoming client requests. Reads upstream proxy configuration
    from the server instance, adds necessary authentication headers,
    and forwards the request using the 'requests' library.
    """
    # Type hint for the server instance to access upstream config
    server: "ThreadedHTTPServerWithConfig"

    # --- Overrides ---
    def __init__(self, request: socket.socket, client_address: Tuple[str, int], server: "ThreadedHTTPServerWithConfig"):
        """Initialize the handler with request, client address, and server."""
        super().__init__(request, client_address, server)

    def log_message(self, format: str, *args: Any) -> None:
        """Route BaseHTTPRequestHandler logging messages through our logger."""
        log.debug(f"Client {self.client_address[0]}:{self.client_address[1]} - {format % args}")

    def log_error(self, format: str, *args: Any) -> None:
        """Route BaseHTTPRequestHandler error messages through our logger."""
        log.error(f"Client {self.client_address[0]}:{self.client_address[1]} - {format % args}")

    # --- Helper Methods ---
    def _get_upstream_auth_header(self) -> Optional[str]:
        """Generates the 'Proxy-Authorization: Basic ...' header value."""
        user = self.server.upstream_user
        pwd = self.server.upstream_password
        if user and pwd:
            credentials = f"{user}:{pwd}"
            token = base64.b64encode(credentials.encode('utf-8')).decode('ascii')
            return f'Basic {token}'
        elif user:
            # Log if user is set but password isn't (might be intentional, but often an error)
            log.warning(f"Client {self.client_address[0]}: Upstream username '{user}' provided but password is missing.")
        return None

    def _filter_headers(self, incoming_headers: Dict[str, str]) -> Dict[str, str]:
        """
        Copies relevant headers from the client request to be sent upstream.
        Filters out hop-by-hop headers and any client-sent proxy auth.
        Ensures the 'Host' header is correctly set for the target URL.
        Removes common IP-revealing headers for privacy.
        """
        outgoing_headers: Dict[str, str] = {}
        # Standard hop-by-hop headers that should not be forwarded directly
        hop_by_hop_headers = {
            'connection', 'keep-alive', 'proxy-authenticate',
            'proxy-authorization', 'te', 'trailers', 'transfer-encoding',
            'upgrade'
        }
        # Headers that might reveal client IP, remove them for privacy
        ip_revealing_headers = {
            'x-forwarded-for', 'x-forwarded-host', 'x-forwarded-proto',
            'x-real-ip', 'via', 'forwarded'
        }

        for key, value in incoming_headers.items():
            lower_key = key.lower()
            if lower_key not in hop_by_hop_headers and lower_key not in ip_revealing_headers:
                outgoing_headers[key] = value

        # Set the Host header correctly for the target resource
        parsed_target_url = urlparse(self.path)
        if parsed_target_url.netloc:
            # Use the host from the absolute URL path if available
            outgoing_headers['Host'] = parsed_target_url.netloc
        elif 'Host' in outgoing_headers:
            # Otherwise, keep the Host header provided by the client
            pass
        else:
            # Log if Host cannot be determined (should be rare with browsers)
            log.warning(f"Client {self.client_address[0]}: Could not determine target Host header for path {self.path}")

        return outgoing_headers

    def _forward_request(self) -> None:
        """
        Forwards the received client request (GET, POST, etc.)
        to the target URL through the configured upstream proxy.
        """
        target_url: str = self.path
        parsed_url = urlparse(target_url)

        # Validate the URL received from the client
        if not parsed_url.scheme or not parsed_url.netloc:
            log.error(f"Client {self.client_address[0]}: Invalid/non-absolute request URL: '{self.path}'")
            self.send_error(400, "Bad Request: Requires absolute URL")
            return

        log.info(f"Client {self.client_address[0]}: Forwarding {self.command} {target_url}")
        log.debug(f"Client {self.client_address[0]}: Incoming Headers: {self.headers}")

        # Prepare headers for the upstream request via 'requests'
        outgoing_headers = self._filter_headers(self.headers)
        auth_header = self._get_upstream_auth_header()
        if auth_header:
            # Add authentication header for the *upstream* proxy
            outgoing_headers['Proxy-Authorization'] = auth_header
        log.debug(f"Client {self.client_address[0]}: Outgoing Headers to Requests: {outgoing_headers}")

        # Read request body if present (e.g., for POST)
        content_length = int(self.headers.get('Content-Length', 0))
        request_body: Optional[bytes] = None
        if content_length > 0:
            try:
                request_body = self.rfile.read(content_length)
                log.debug(f"Client {self.client_address[0]}: Read {content_length} bytes request body.")
            except Exception as e:
                 log.error(f"Client {self.client_address[0]}: Failed to read request body: {e}")
                 self.send_error(400, "Bad Request: Could not read request body")
                 return


        # Configure the upstream proxy URL for the 'requests' library
        upstream_proxy_url = f"http://{self.server.upstream_host}:{self.server.upstream_port}"
        proxies = {
            'http': upstream_proxy_url,
            'https': upstream_proxy_url # Use same upstream proxy for http/https targets
        }
        log.debug(f"Client {self.client_address[0]}: Using upstream proxy for requests: {proxies}")

        response: Optional[requests.Response] = None
        try:
            # Make the request using the 'requests' library
            # 'requests' handles the connection *through* the upstream proxy specified in 'proxies'
            response = requests.request(
                method=self.command,
                url=target_url,
                headers=outgoing_headers,
                data=request_body,
                proxies=proxies,
                stream=True,          # Enable streaming for response body
                verify=False,         # Disable SSL verification for the TARGET URL. Set path to CA bundle if needed.
                allow_redirects=False,# Let the client (browser) handle redirects
                timeout=60            # Add a timeout (seconds)
            )

            log.info(f"Client {self.client_address[0]}: Upstream response {response.status_code} for {target_url}")
            log.debug(f"Client {self.client_address[0]}: Response Headers from upstream: {response.headers}")

            # --- Send response back to client ---
            # 1. Send status line
            self.send_response(response.status_code)

            # 2. Send response headers (filtered)
            response_headers = self._filter_headers(response.headers)
            for key, value in response_headers.items():
                self.send_header(key, value)
            self.end_headers()

            # 3. Stream response body
            if response.raw:
                 try:
                    # Use copyfileobj for efficient streaming
                    shutil.copyfileobj(response.raw, self.wfile)
                    log.debug(f"Client {self.client_address[0]}: Finished streaming response body.")
                 except ConnectionResetError:
                     log.warning(f"Client {self.client_address[0]} disconnected before response finished.")
                 except Exception as e:
                     # Log errors occurring while writing back to client
                     log.exception(f"Client {self.client_address[0]}: Error writing response body:")

        except requests.exceptions.ProxyError as e:
            log.error(f"Client {self.client_address[0]}: Upstream Proxy Error connecting to {upstream_proxy_url}: {e}")
            self.send_error(502, f"Bad Gateway - Upstream Proxy Error")
        except requests.exceptions.Timeout as e:
             log.error(f"Client {self.client_address[0]}: Timeout connecting to target or upstream proxy: {e}")
             self.send_error(504, "Gateway Timeout")
        except requests.exceptions.SSLError as e:
             # This relates to the SSL certificate of the *target* server
             log.error(f"Client {self.client_address[0]}: SSL Error connecting to target {target_url} (verify=False?): {e}")
             self.send_error(502, f"Bad Gateway - SSL Error for Target")
        except requests.exceptions.RequestException as e:
            log.error(f"Client {self.client_address[0]}: Error forwarding request to {target_url}: {e}")
            self.send_error(502, f"Bad Gateway - Request Forwarding Error")
        except Exception as e:
            # Catch any other unexpected errors during request handling
            log.exception(f"Client {self.client_address[0]}: Unexpected error handling request:")
            try:
                # Try to send a generic 500 error back to the client
                self.send_error(500, f"Internal Server Error")
            except Exception:
                 # Avoid errors during error reporting itself
                 log.error(f"Client {self.client_address[0]}: Failed to send error response to client.")
        finally:
            # Ensure the response is closed to release resources
             if response:
                 response.close()
                 log.debug(f"Client {self.client_address[0]}: Closed upstream response object.")


    # --- HTTP Method Handlers ---
    def do_CONNECT(self):
        """
        Handles HTTPS CONNECT requests by establishing a tunnel between the client
        and the upstream proxy.
        """
        # Parse the target address from the path (host:port)
        target = self.path.split(':')
        if len(target) != 2:
            self.send_error(400, "Bad CONNECT request format (expected host:port)")
            return
            
        target_host, target_port = target
        try:
            target_port = int(target_port)
        except ValueError:
            self.send_error(400, f"Invalid port number: {target[1]}")
            return
            
        log.info(f"Client {self.client_address[0]}:{self.client_address[1]} - CONNECT tunnel to {target_host}:{target_port}")
        
        # Create connection to the upstream proxy
        upstream_socket = None
        try:
            # Connect to the upstream proxy
            upstream_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            upstream_socket.settimeout(60)  # Set timeout to avoid hanging
            try:
                upstream_socket.connect((self.server.upstream_host, self.server.upstream_port))
            except Exception as e:
                log.error(f"Failed to connect to upstream proxy: {str(e)}")
                self.send_error(502, f"Unable to connect to upstream proxy: {str(e)}")
                return
            
            # Send CONNECT request to the upstream proxy
            connect_request = f"CONNECT {target_host}:{target_port} HTTP/1.1\r\n"
            connect_request += f"Host: {target_host}:{target_port}\r\n"
            
            # Add proxy authentication if provided
            auth_header = self._get_upstream_auth_header()
            if auth_header:
                connect_request += f"Proxy-Authorization: {auth_header}\r\n"
                
            # Add empty line to end headers
            connect_request += "\r\n"
            
            try:
                # Send the CONNECT request to upstream proxy
                upstream_socket.sendall(connect_request.encode('utf-8'))
            except Exception as e:
                log.error(f"Failed to send CONNECT request to upstream: {str(e)}")
                self.send_error(502, f"Failed sending CONNECT request: {str(e)}")
                return
            
            # Read the response from upstream proxy with timeout protection
            response = b''
            try:
                while b'\r\n\r\n' not in response:
                    data = upstream_socket.recv(4096)
                    if not data:
                        break
                    response += data
            except socket.timeout:
                log.error(f"Timeout reading response from upstream proxy")
                self.send_error(504, "Timeout reading from upstream proxy")
                if upstream_socket:
                    upstream_socket.close()
                return
            except Exception as e:
                log.error(f"Error reading response from upstream: {str(e)}")
                self.send_error(502, f"Error reading from upstream: {str(e)}")
                if upstream_socket:
                    upstream_socket.close()
                return
                
            # Check if CONNECT was successful (should contain 200 status)
            response_str = response.decode('utf-8', errors='ignore')
            if '200' not in response_str.split('\r\n')[0]:
                log.error(f"Upstream proxy rejected CONNECT request: {response_str.strip()}")
                self.send_error(502, "Upstream proxy rejected CONNECT")
                if upstream_socket:
                    upstream_socket.close()
                return
                
            # If we got here, the upstream proxy accepted our CONNECT request
            try:
                # Send 200 Connection Established to the client
                self.send_response(200, 'Connection Established')
                self.send_header('Connection', 'keep-alive')
                self.end_headers()
            except Exception as e:
                log.error(f"Error sending 200 response to client: {str(e)}")
                if upstream_socket:
                    upstream_socket.close()
                return
            
            # Now set up bidirectional tunneling between client and upstream
            client_socket = self.connection
            
            # Set client socket to non-blocking mode
            client_socket.settimeout(None)
            client_socket.setblocking(0)
            
            # Set upstream socket to non-blocking mode
            upstream_socket.settimeout(None)
            upstream_socket.setblocking(0)
            
            # Import select for socket monitoring
            import select
            
            # Initial buffer sizes and timeout settings
            buffer_size = 8192
            socket_timeout = 60  # 60 seconds
            
            # Create buffers for any data waiting to be sent
            client_buffer = b''
            upstream_buffer = b''
            
            # Create connection monitoring lists
            inputs = [client_socket, upstream_socket]
            outputs = []
            
            # Start time for timeout tracking
            start_time = time.time()
            
            # Tunnel loop
            try:
                while inputs:
                    # Check for timeout
                    if time.time() - start_time > socket_timeout:
                        log.debug(f"Tunnel timeout for {target_host}:{target_port}")
                        break
                    
                    # Wait for readable/writable sockets
                    readable, writable, exceptional = select.select(
                        inputs, outputs, inputs, 1.0  # Use a short timeout to check for timeout
                    )
                    
                    # Handle readable sockets (data available)
                    for sock in readable:
                        try:
                            # If this is the client socket, read from client and queue to upstream
                            if sock is client_socket:
                                data = sock.recv(buffer_size)
                                if data:
                                    upstream_buffer += data
                                    if upstream_socket not in outputs:
                                        outputs.append(upstream_socket)
                                    # Reset timeout on activity
                                    start_time = time.time()
                                else:
                                    # Client closed connection, tunnel is done
                                    inputs.remove(client_socket)
                                    if client_socket in outputs:
                                        outputs.remove(client_socket)
                                    client_socket.close()
                                    
                            # If this is the upstream socket, read from upstream and queue to client
                            elif sock is upstream_socket:
                                data = sock.recv(buffer_size)
                                if data:
                                    client_buffer += data
                                    if client_socket not in outputs:
                                        outputs.append(client_socket)
                                    # Reset timeout on activity
                                    start_time = time.time()
                                else:
                                    # Upstream closed connection, tunnel is done
                                    inputs.remove(upstream_socket)
                                    if upstream_socket in outputs:
                                        outputs.remove(upstream_socket)
                                    upstream_socket.close()
                        except (ConnectionError, OSError) as e:
                            log.error(f"Connection error during tunneling: {str(e)}")
                            if sock in inputs:
                                inputs.remove(sock)
                            if sock in outputs:
                                outputs.remove(sock)
                            sock.close()
                            break
                    
                    # Handle writable sockets (can send data)
                    for sock in writable:
                        try:
                            if sock is client_socket and client_buffer:
                                # Send data to client
                                sent = sock.send(client_buffer)
                                # Remove sent data from buffer
                                client_buffer = client_buffer[sent:]
                                # If buffer is empty, stop monitoring for write
                                if not client_buffer and sock in outputs:
                                    outputs.remove(sock)
                                
                            elif sock is upstream_socket and upstream_buffer:
                                # Send data to upstream
                                sent = sock.send(upstream_buffer)
                                # Remove sent data from buffer
                                upstream_buffer = upstream_buffer[sent:]
                                # If buffer is empty, stop monitoring for write
                                if not upstream_buffer and sock in outputs:
                                    outputs.remove(sock)
                        except (ConnectionError, OSError) as e:
                            log.error(f"Connection error during data forwarding: {str(e)}")
                            if sock in inputs:
                                inputs.remove(sock)
                            if sock in outputs:
                                outputs.remove(sock)
                            sock.close()
                            break
                    
                    # Handle exceptional conditions
                    for sock in exceptional:
                        log.error(f"Exception condition on socket during tunneling")
                        if sock in inputs:
                            inputs.remove(sock)
                        if sock in outputs:
                            outputs.remove(sock)
                        sock.close()
                
                log.debug(f"Tunnel closed gracefully for {target_host}:{target_port}")
                
            except Exception as e:
                log.error(f"Error during tunnel operation: {str(e)}")
            finally:
                # Clean up any remaining socket connections
                for sock in [s for s in [client_socket, upstream_socket] if s and s not in [None]]:
                    try:
                        if sock in inputs:
                            inputs.remove(sock)
                        if sock in outputs:
                            outputs.remove(sock)
                        sock.close()
                    except:
                        pass
        
        except Exception as e:
            log.error(f"Error establishing CONNECT tunnel to {target_host}:{target_port}: {str(e)}")
            self.send_error(502, f"Error connecting to upstream proxy: {str(e)}")
        finally:
            # Ensure the upstream socket is closed if still exists
            if upstream_socket:
                try:
                    upstream_socket.close()
                except:
                    pass

    def do_GET(self):
        self._forward_request()

    def do_POST(self):
        self._forward_request()

    def do_HEAD(self):
        self._forward_request()

    def do_PUT(self):
        self._forward_request()

    def do_DELETE(self):
        self._forward_request()

    def do_OPTIONS(self):
        # For OPTIONS, we might want a different behavior or just forward
        # For simplicity, forwarding it. A more robust proxy might handle OPTIONS directly.
        self._forward_request()

    def do_PATCH(self):
        self._forward_request()


# --- Threaded Server Class ---
class ThreadedHTTPServerWithConfig(socketserver.ThreadingMixIn, HTTPServer):
    """
    A threaded HTTP server that stores upstream proxy configuration
    and makes it accessible to request handlers.
    """
    allow_reuse_address = True  # Allow quick restarts of the server

    # Store upstream configuration details
    upstream_host: str
    upstream_port: int
    upstream_user: Optional[str]
    upstream_password: Optional[str]

    def __init__(self,
                 server_address: Tuple[str, int],
                 RequestHandlerClass: Type[BaseHTTPRequestHandler],
                 upstream_config: Dict[str, Any]):
        """
        Initialize the server, storing the upstream proxy configuration.
        """
        # Store upstream proxy configuration directly on the server instance
        self.upstream_host = upstream_config['host']
        self.upstream_port = upstream_config['port']
        self.upstream_user = upstream_config['user']
        self.upstream_password = upstream_config['password']
        log.debug(f"Server initialized with upstream target: {self.upstream_host}:{self.upstream_port}")
        # Call the base class constructor
        super().__init__(server_address, RequestHandlerClass)


# --- Server Control Class ---
class LocalProxyRunner:
    """
    Manages the lifecycle of the local proxy server thread.
    Reads upstream configuration primarily from environment variables upon initialization.
    Provides methods to start, stop, and get the proxy address.
    """
    local_host: str
    local_port: int
    upstream_host: str
    upstream_port: int
    upstream_user: Optional[str]
    upstream_password: Optional[str]
    server: Optional[ThreadedHTTPServerWithConfig]
    server_thread: Optional[threading.Thread]

    # Class-level dictionary to track IPs assigned to different proxy ports
    _assigned_ips = {}
    _assigned_ips_lock = threading.Lock()
    
    def __init__(self, local_host: str = "127.0.0.1", local_port: int = 8081):
        """
        Initializes the runner, reading upstream config from environment variables.
        Raises EnvironmentError if required variables are missing or invalid.
        """
        self.local_host = local_host
        self.local_port = local_port
        self.server = None
        self.server_thread = None

        # Ensure we have the latest environment variables by reloading .env
        try:
            from dotenv import load_dotenv
            load_dotenv(override=True)  # override=True ensures we get fresh values
            log.debug("Reloaded .env file to ensure fresh environment variables")
        except ImportError:
            log.debug("python-dotenv not available, using system environment variables")

        # --- Read upstream config from environment variables ---
        log.debug("Reading upstream proxy configuration from environment variables...")
        upstream_host_env = os.environ.get("PROXY_HOST")
        upstream_port_env = os.environ.get("PROXY_PORT")
        self.upstream_user = os.environ.get("PROXY_USERNAME")
        self.upstream_password = os.environ.get("PROXY_PASSWORD")
        # ---

        # --- Validate essential configuration ---
        if not upstream_host_env:
            raise EnvironmentError("PROXY_HOST environment variable not set.")
        if not upstream_port_env:
             raise EnvironmentError("PROXY_PORT environment variable not set.")

        self.upstream_host = upstream_host_env # Store validated host
        try:
            self.upstream_port = int(upstream_port_env) # Convert port to integer
        except ValueError:
             raise EnvironmentError(f"Invalid PROXY_PORT value: '{upstream_port_env}'. Must be an integer.")

        # Log the configuration being used (mask password)
        log.info("Local Proxy Runner Initialized:")
        log.info(f"  Listen Address : {self.local_host}:{self.local_port}")
        log.info(f"  Upstream Proxy : {self.upstream_host}:{self.upstream_port}")
        log.info(f"  Upstream User  : {'******' if self.upstream_user else 'Not Set'}")
        # Add warning if user is set but password is not
        if self.upstream_user and not self.upstream_password:
            log.warning("PROXY_USERNAME is set, but PROXY_PASSWORD is not set in environment.")

    def get_proxy_address(self) -> str:
        """Returns the address string (host:port) for client configuration."""
        return f"{self.local_host}:{self.local_port}"

    def get_proxy_url(self) -> str:
        """Returns the full URL (scheme://host:port) for client configuration."""
        return f"http://{self.local_host}:{self.local_port}"
        
    def check_basic_connectivity(self) -> bool:
        """Performs a basic connectivity test through the upstream proxy."""
        log.info(f"Performing basic connectivity test for upstream proxy {self.upstream_host}:{self.upstream_port}...")
        test_target_url = "http://httpbin.org/get" # Use a reliable, simple target
        upstream_proxy_url = f"http://{self.upstream_host}:{self.upstream_port}"
        proxies = {
            'http': upstream_proxy_url,
            'https': upstream_proxy_url
        }
        
        auth = None
        if self.upstream_user and self.upstream_password:
            auth = requests.auth.HTTPProxyAuth(self.upstream_user, self.upstream_password)
            
        try:
            # Send a HEAD request (less data transfer) through the upstream proxy
            response = requests.head(
                test_target_url,
                proxies=proxies,
                auth=auth,
                timeout=20, # Shorter timeout for basic check
                verify=False, # Match verify setting if needed, but often OK for basic check
                allow_redirects=False
            )
            response.raise_for_status() # Check for HTTP errors
            log.info(f"Basic connectivity test successful (Status: {response.status_code})")
            return True
        except requests.exceptions.RequestException as e:
            log.error(f"Basic connectivity test FAILED: {e}")
            return False
        except Exception as e:
            log.error(f"Unexpected error during basic connectivity test: {e}")
            return False
            
    def check_ip_assignment(self) -> Tuple[str, bool]:
        """Checks the current IP associated with this proxy instance and verifies its uniqueness.
        Returns: Tuple of (IP address, boolean indicating if IP is unique across instances)
        """
        ip = self._get_current_ip()
        is_unique = self._verify_ip_uniqueness(ip)
        return ip, is_unique
    
    def _get_current_ip(self) -> str:
        """Get the current public IP used by this proxy instance"""
        log.info(f"Checking IP for proxy on port {self.local_port}...")
        test_target_url = "http://httpbin.org/ip"
        test_proxies = {
            'http': self.get_proxy_url(),
            'https': self.get_proxy_url()
        }
        
        try:
            response = requests.get(test_target_url, proxies=test_proxies, timeout=30, verify=False)
            response.raise_for_status()
            ip_data = response.json()
            current_ip = ip_data.get('origin')
            if not current_ip:
                raise ValueError("Could not determine IP from response")
                
            # Store this IP in the class-level tracker
            with self._assigned_ips_lock:
                self._assigned_ips[self.local_port] = current_ip
                
            log.info(f"Proxy on port {self.local_port} is using IP: {current_ip}")
            return current_ip
        except Exception as e:
            log.error(f"Failed to get IP for proxy on port {self.local_port}: {e}")
            raise
    
    def _verify_ip_uniqueness(self, current_ip: str) -> bool:
        """Verify that this proxy instance has a unique IP compared to other instances"""
        with self._assigned_ips_lock:
            # Check if any other proxy instance is using the same IP
            other_instances_with_same_ip = [port for port, ip in self._assigned_ips.items() 
                                          if ip == current_ip and port != self.local_port]
            
            is_unique = len(other_instances_with_same_ip) == 0
            if not is_unique:
                log.warning(f"IP UNIQUENESS WARNING: Proxy on port {self.local_port} has the same IP {current_ip} "
                          f"as proxies on ports {other_instances_with_same_ip}")
            else:
                log.info(f"Proxy on port {self.local_port} has a unique IP: {current_ip}")
                
            return is_unique

    def start(self) -> None:
        """Starts the local proxy server in a separate daemon thread."""
        if self.server_thread and self.server_thread.is_alive():
            log.warning("Proxy server thread is already running.")
            return

        # Prepare upstream config dictionary for the server instance
        upstream_config = {
            'host': self.upstream_host,
            'port': self.upstream_port,
            'user': self.upstream_user,
            'password': self.upstream_password
        }

        # Create the threaded server instance
        self.server = ThreadedHTTPServerWithConfig(
            (self.local_host, self.local_port), # Local address to bind to
            UpstreamProxyRequestHandler,        # Request handler class
            upstream_config                     # Upstream proxy details
        )

        # Create and start the server thread
        # daemon=True allows the main program to exit even if this thread is running
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.name = f"LocalProxyThread-{self.local_port}" # Set thread name
        self.server_thread.start()
        log.info(f"Local proxy server started on {self.get_proxy_url()} [Thread: {self.server_thread.name}]")

    def stop(self) -> None:
        """Stops the local proxy server gracefully."""
        if not self.server or not self.server_thread or not self.server_thread.is_alive():
            log.info("Proxy server is not running or already stopped.")
            return

        log.info(f"Shutting down local proxy server on {self.get_proxy_url()}...")
        try:
            self.server.shutdown() # Signal the serve_forever loop to exit
            self.server.server_close() # Close the listening socket
            # Wait for the server thread to finish, with a timeout
            self.server_thread.join(timeout=5.0)
            if self.server_thread.is_alive():
                 log.warning("Server thread did not shut down within timeout.")
        except Exception as e:
            # Log any errors during the shutdown process
            log.exception("Error during server shutdown:")
        finally:
            # Clean up references
            self.server = None
            self.server_thread = None
            log.info("Local proxy server stopped.")
            
    def stop_proxy(self) -> None:
        """Alias for stop() method to maintain compatibility."""
        return self.stop()

# --- Example Usage (when script is run directly) ---
if __name__ == '__main__':
    import time
    import json # Needed to parse httpbin response

    print("--- Local Proxy Server Test Runner ---")

    # Check if required environment variables are set before proceeding
    required_env_vars = ["PROXY_HOST", "PROXY_PORT", "PROXY_USERNAME", "PROXY_PASSWORD"]
    missing_vars = [v for v in required_env_vars if v not in os.environ]

    if missing_vars:
        print("*" * 40)
        print(" ERROR: Required environment variables missing! ")
        print(" Please set the following environment variables:")
        for var in required_env_vars:
            print(f"  - {var:<20} {'(Set)' if var not in missing_vars else '(Missing!)'}")
        print("*" * 40)
        exit(1) # Exit if configuration is incomplete
    else:
         log.info("Found required proxy environment variables.")

    # Define the local port for the proxy server
    LOCAL_PORT_TO_USE = 8081 # Port your Selenium script will connect to

    proxy_runner: Optional[LocalProxyRunner] = None
    try:
        # Initialize the proxy runner (reads env vars in __init__)
        proxy_runner = LocalProxyRunner(local_port=LOCAL_PORT_TO_USE)

        # Start the proxy server in its thread
        proxy_runner.start()

        local_proxy_url = proxy_runner.get_proxy_url()
        print(f"\nLocal proxy is RUNNING on: {local_proxy_url}")
        print("Configure Selenium/clients to use this URL.")
        # print(f"Example Selenium option: --proxy-server={local_proxy_url}")

        # --- IP Leak Test ---
        print("\n--- Performing IP Leak Test ---")
        test_target_url = "http://httpbin.org/ip"
        real_ip = None
        proxied_ip = None

        # 1. Get Real Public IP (no proxy)
        try:
            print(f"Checking real IP using GET {test_target_url}...")
            # Increased timeout slightly, disable SSL verification if needed for corporate networks etc.
            response_real = requests.get(test_target_url, timeout=15, verify=True)
            response_real.raise_for_status()
            real_ip_data = response_real.json()
            real_ip = real_ip_data.get('origin')
            if real_ip:
                print(f"  Real Public IP: {real_ip}")
            else:
                print("  Could not determine real public IP from response.")
        except requests.exceptions.RequestException as e:
            print(f"  ERROR getting real IP: {e}")
        except json.JSONDecodeError:
            print("  ERROR: Could not parse JSON response for real IP.")
        except Exception as e:
            print(f"  An unexpected error occurred getting real IP: {e}")

        # 2. Get IP via Local Proxy
        if local_proxy_url:
            test_proxies = {
                'http': local_proxy_url,
                'https': local_proxy_url, # Keep this for completeness, though CONNECT fails
            }
            try:
                print(f"Checking proxied IP using GET {test_target_url} via {local_proxy_url}...")
                # Note: verify=False might be needed if upstream proxy uses self-signed certs internally,
                # but httpbin itself should be fine. Set to False for robustness with various proxies.
                response_proxied = requests.get(test_target_url, proxies=test_proxies, timeout=30, verify=False)
                response_proxied.raise_for_status() # Check for HTTP errors (4xx or 5xx)
                proxied_ip_data = response_proxied.json()
                proxied_ip = proxied_ip_data.get('origin')
                if proxied_ip:
                     print(f"  IP via Proxy:   {proxied_ip}")
                else:
                    print("  Could not determine proxied IP from response.")
            except requests.exceptions.RequestException as e:
                print(f"\n  ERROR: Proxied request FAILED: {e}")
                log.error(f"Proxied test request failed: {e}")
            except json.JSONDecodeError:
                 print("  ERROR: Could not parse JSON response for proxied IP.")
            except Exception as e:
                print(f"\n  ERROR: An unexpected error occurred during proxied test: {e}")
                log.exception("Unexpected proxied test error:")

        # 3. Compare IPs and Report
        print("\n--- IP Leak Test Result ---")
        if real_ip and proxied_ip:
            if real_ip != proxied_ip:
                print(f"SUCCESS: Proxy appears to be working. Real IP ({real_ip}) differs from Proxied IP ({proxied_ip}).")
            else:
                print(f"!!! WARNING: POTENTIAL IP LEAK !!! Real IP ({real_ip}) is the SAME as Proxied IP ({proxied_ip}).")
                print("  This could mean the upstream proxy is not working or is transparent.")
        elif not real_ip:
            print("Could not determine Real IP. Cannot perform comparison.")
        elif not proxied_ip:
            print("Could not determine IP via Proxy (request may have failed). Cannot perform comparison.")
        else:
            print("Could not determine one or both IPs. Cannot perform comparison.")


        # Keep the main thread alive while the proxy runs
        print("\nProxy server running in background. Press Ctrl+C to stop.")
        while True:
            time.sleep(1)

    except EnvironmentError as e:
        # Handle errors during initialization (missing env vars)
        print(f"\nERROR: Configuration Error - {e}")
        log.critical(f"Failed to initialize LocalProxyRunner: {e}")
        exit(1)
    except KeyboardInterrupt:
        # Handle Ctrl+C gracefully
        print("\nCtrl+C received. Shutting down...")
    except Exception as e:
        # Catch any other unexpected errors in the main block
        log.exception("An unexpected error occurred in the main execution block:")
    finally:
        # Ensure the proxy server is stopped on exit
        if proxy_runner:
            proxy_runner.stop()
        print("Exiting.")