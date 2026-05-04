# GitHub Actions Setup

CI/CD for the ASB PL Application Scorecard MLOps framework. Two workflows:

| Workflow | Trigger | Purpose |
|---|---|---|
| `ml-tests-ci.yml` | push to `main` | Deploy bundle to **stg** → run ETL + 3 ML jobs end-to-end → auto-PR `main` → `release` |
| `ml-bundle-cd.yml` | push to `release` | Deploy bundle (job definitions only) to **prd** |

## One-time GitHub repo setup

### 1. Create two GitHub Environments

Settings → Environments → **New environment**

#### Environment: `staging`

Add secrets (Settings → Environments → staging → Add secret):

| Secret | Value |
|---|---|
| `DATABRICKS_HOST` | `https://adb-7405610706859452.12.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | stg PAT or service principal token |

No protection rules required; CI is automated.

#### Environment: `production`

Add the same secrets (with prd values):

| Secret | Value |
|---|---|
| `DATABRICKS_HOST` | `https://adb-7405614555582503.3.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | prd CD service principal token |

**Recommended protection rules:**

- ✅ Required reviewers — add 1+ designated approvers
- ✅ Deployment branches — restrict to `release`
- ✅ Wait timer (optional) — short delay before deploy starts

### 2. Branch protection on `main` and `release`

Settings → Branches → Add rule:

- `main`: require PR review, require status checks (CI must pass), no direct pushes
- `release`: require PR review, restrict pushes to repository admins + the CI bot

### 3. Branches must exist

The workflows reference both `main` and `release`. After importing the repo, create the `release` branch from `main` once so CD can deploy the first time:

```bash
git checkout main
git checkout -b release
git push origin release
```

## What gets deployed where

| Environment | Catalog | Workspace | Volume |
|---|---|---|---|
| stg | `stg_retail_modelling` | `https://adb-7405610706859452.12.azuredatabricks.net` | 30K applications |
| prd | `prod_retail_modelling` | `https://adb-7405614555582503.3.azuredatabricks.net` | 100K applications |

Local development uses `dev` target (catalog `dev_retail_modelling`), deployed manually via CLI; not part of CI/CD.

## Verifying the workflows

After pushing to `main` for the first time:

1. Go to **Actions** tab → look for `ML CI - Deploy & Test on Staging`
2. Workflow triggers automatically; takes ~15-20 min (CLI install + bundle deploy + 4 jobs)
3. On success, an auto-PR `main` → `release` is created
4. Approve and merge the PR → triggers `ML CD - Deploy to Production`
5. CD validates + deploys job definitions to prd. Jobs do NOT run automatically; they execute on their schedules.

## Service principal vs PAT (recommended for prd)

The `DATABRICKS_TOKEN` for the `production` environment **should be a service principal token**, not a personal PAT. Reasons:

- Survives the user leaving the org
- Audit logs show "deployed by CD service principal", not "deployed by Srinu"
- Aligns with APRA/RBNZ separation of duties (humans never deploy to prod)

Create the SP in Databricks → Workspace settings → Service principals → generate token → store in the `production` GitHub Environment.

For `staging`, a PAT is fine during build-out; switch to a SP when production-ready.
