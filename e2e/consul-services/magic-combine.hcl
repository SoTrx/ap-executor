// Consul service definition for "Magic Combine", loaded by a plain Consul
// client agent (`agent -client -config-dir=...`) via the e2e-consul-agent-c
// container in docker-compose.e2e.yml. The agent registers this service and
// runs the health check itself, purely from this file -- no application
// code involved.
service {
  name    = "magic-combine"
  id      = "magic-combine"
  address = "e2e-magic-c"
  port    = 8000

  meta = {
    version = "1.0.0"
  }

  check {
    http                              = "http://e2e-magic-c:8000/health"
    interval                          = "10s"
    timeout                           = "5s"
    deregister_critical_service_after = "1m"
  }
}
