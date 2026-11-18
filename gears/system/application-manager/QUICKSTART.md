# Application Manager - Quickstart

The Application Manager records which applications are installed and
dispatches each section of an application's manifest to the section plugin
that owns it. Deployment tools seed and uninstall on the platform plane;
tenants read the installed applications and the operation history.

**Features:**
- Seed an application from a manifest, or uninstall it, as an operation that
  can be polled
- Section plugins in priority order, each owning one section key and its
  schema
- Discovery of the section keys, each section schema, and the combined
  manifest schema
- A paged list of installed applications and the operation history

Full API documentation: <http://127.0.0.1:8087/cf/docs>

## Examples

### List installed applications

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  http://127.0.0.1:8087/cf/application-manager/v1/applications | python3 -m json.tool
```

### Read the history of one operation

```bash
curl -s -G -H "Authorization: Bearer $TOKEN" \
  --data-urlencode '$filter=operation_id eq 019a0000-0000-7000-8000-000000000000' \
  http://127.0.0.1:8087/cf/application-manager/v1/operation-history | python3 -m json.tool
```

Seed, uninstall, the operation read, and the schema reads take a platform
identity named in `allowed_callers`; see [README.md](README.md).

For additional endpoints, see <http://127.0.0.1:8087/cf/docs>.
