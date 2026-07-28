// Consul service definition for "Magic Echo A", loaded by a plain Consul
// client agent (`agent -client -config-dir=...`) via the e2e-consul-agent-a
// container in docker-compose.e2e.yml. The agent registers this service and
// runs the health check itself, purely from this file -- no application
// code involved.
service {
  name    = "magic-echo-a"
  id      = "magic-echo-a"
  address = "e2e-magic-a"
  port    = 8000

  meta = {
    version = "1.0.0"
  }

  check {
    http                              = "http://e2e-magic-a:8000/health"
    interval                          = "10s"
    timeout                           = "5s"
    deregister_critical_service_after = "1m"
  }
}
