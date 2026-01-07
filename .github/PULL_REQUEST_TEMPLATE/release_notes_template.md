# Release Notes

List the key changes in this release using bullet points:
- New features added (with brief description of what they enable)
- Enhancements to existing functionality
- Bug fixes (reference PR/issue numbers)
- Breaking changes (if any - mark clearly with ⚠️)
- Dependency updates (library versions)

# Plans for Customer Communication

Describe how and when customers will be notified about this release:
- Which communication channels will be used (changelog, email, in-app notification)
- Timeline for announcement (before/during/after deployment)
- Whether this requires customer action or is transparent
- Link to public changelog entry or documentation if applicable

# Impact Analysis

Assess the impact of this release on users and systems:
- Which user groups are affected (all users, local deployment users, specific MCP clients)
- Whether users need to take action (update configs, migrate settings, etc.)
- Potential service interruption or downtime during deployment
- Risk level (Low/Medium/High) and why

# Change Type

Classify the release type:
- Major (breaking changes, significant new features)
- Minor (new features, enhancements, backward compatible)
- Patch (bug fixes, small improvements, no new features)

# Justification

Explain why this release is necessary:
- What problem does it solve or what need does it address
- Business or technical drivers for the changes
- User feedback or requests that motivated features
- Dependencies or external factors (e.g., protocol updates, security requirements)

# Testing

This section is to be filled by the release testers. Leave it as it is and just remove this instruction text.

- [ ] Tested with Cursor AI desktop (all transports)
- [ ] Tested with claude.ai web and canary-orion MCP (SSE and Streamable-HTTP)
- [ ] Tested with In Platform Agent on canary-orion
- [ ] Tested with RO chat on canary-orion

# Deployment Plan

Outline the deployment process:
- Deployment method (automated CI/CD, manual steps)
- Timing (date/time window for deployment)
- Order of operations (which environments: dev → staging → production)
- Any required coordination with other teams or systems
- Verification steps after deployment

# Rollback Plan

Define how to revert if issues occur:
- Steps to roll back to previous version
- Conditions that would trigger a rollback
- Data migration rollback (if applicable)
- Who has authority to initiate rollback
- Estimated time to complete rollback

# Post-Release Support Plan

Describe monitoring and support after release:
- What metrics/logs to monitor for issues
- On-call or support coverage plan
- Known issues to watch for
- Timeline for post-release monitoring (e.g., 24-48 hours)
- Escalation path if critical issues are found