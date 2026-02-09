# AGENTS.md

Project expectations for automated agents:

- Use `deps.edn` for dependency management.
- Use babashka tasks for developer workflows (`bb test`, `bb lint`, `bb maint`).
- Run `bb lint` and `bb test` regularly, especially after changes.
- Tests run via Kaocha and should include generative coverage when appropriate.
- Linting is performed with clj-kondo.
- Keep namespaces under the `com.latacora.*` prefix.
- Prefer functional Clojure style and threading macros where they improve clarity.
