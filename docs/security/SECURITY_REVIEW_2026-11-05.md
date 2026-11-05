# Legacy Iceberg MSK Connect: security assessment

**Assessment date:** 2026-11-05  
**Scope:** verified production-candidate plugin ZIP, pinned source, build configuration, and the supplied MSK Connect deployment context.  
**Status:** findings and recommendations only. No dependency changes, rebuilt release, deployment, or production approval is included.

## Decision summary

The verified baseline is **not a clean vulnerability scan**. Trivy 0.76.1 reported **89 package/advisory matches across 22 JARs**, representing **74 distinct CVEs**. The package-match totals are **2 critical, 43 high, 40 medium, and 4 low**. Counting each CVE once gives 2 critical, 34 high, 34 medium, and 4 low. These are scanner severities, not 89 demonstrated exploit paths.

The most important issues for a temporary production decision are:

- Parquet 1.13.1 has critical/high schema-deserialization advisories. The connector is primarily a writer; exploitation through the affected Parquet reader has **not** been demonstrated in this deployment.
- Netty 4.1.124.Final has numerous advisories. Its critical SNI finding requires a server configuration not established for this outbound Glue/S3 client, but other client-side findings need separate assessment.
- Both standalone and shaded Jackson/Guava copies require attention. Updating a standalone dependency cannot replace classes relocated inside another JAR.
- Compression libraries have memory-safety, information-disclosure, or denial-of-service findings. Reachability depends on the particular decompression/native API used.
- Manual review found schema-directed class initialization in the control-message decoder, before the connector checks the message's group ID. This is a separate code finding, not a claim that the fixed Avro CVE still affects Avro 1.11.4.
- The tested custom packaging recipe and Glue inputs are external to this repository. The Git commit alone does not reproduce the tested dependency set.
- MSK's worker Kafka version has separate upstream advisories. AWS backport/patch status is unknown and cannot be established from the version string or this plugin scan.

**Recommendation:** remediate and rescan before deployment where feasible. If the unchanged artifact must be used as a stopgap, security should approve an explicit, time-limited exception for this exact SHA-256, with the controls and evidence below. This assessment does not establish that all high/critical findings are exploitable, or that their absence from an observed path makes the artifact safe. No acceptance decision has been made.

## 1. Artifact identity and production context

| Item | Assessed value / provenance |
| --- | --- |
| Source commit | `4ac3ebb15f72ae1fd3459623c17b5b14daadab4c`; verified locally |
| Source | `git@github.com:jord-bere-scacap/iceberg-kafka-connect.git` |
| Distribution | Custom standard `distZip`, not `hiveDistZip`; recovered handoff and archive layout agree |
| Generated connector version | `0.1.0-dev.320+4ac3ebb` |
| Iceberg version | `1.5.2` |
| Glue Schema Registry converter | `1.1.27`; this is not the connector version |
| Published filename | `iceberg-kafka-connect-glue-1.1.27-msk-connect.zip` |
| S3 bucket | `dev-msk-connect-plugin-artifacts.scalable-dev` |
| S3 key | `iceberg-kafka-connect/legacy/4ac3ebb15f72ae1fd3459623c17b5b14daadab4c/iceberg-kafka-connect-glue-1.1.27-msk-connect.zip` |
| S3 VersionId | `eAIYVb.Y_eV_YLCjDyapK36JnqELceT7` — supplied by handoff |
| ZIP SHA-256 | `0deb3d936bce77a69bd969fcea01f529a1068254256ec1828577775d3f6bd414` — independently verified against local bytes |
| ZIP size | 96,987,652 bytes; 172 JARs |
| Build recipe SHA-256 | `fde898f59359bc9e47a3f1293caf4ee76fc100fba4fd2bc86de7844061f3445f` — verified for recovered `build.init.gradle` |
| Worker | AWS-managed MSK Connect `3.7.x`; supplied logs report Kafka `3.7.0` |
| Worker JVM | Supplied logs report Amazon OpenJDK `17.0.20.1+13-LTS` |
| Build tools | Gradle `8.1.1`; recovered Temurin JDK `17.0.21+9` |
| Integration | `GlueCatalog`, `S3FileIO`, Iceberg format v2, Lake Formation access |
| Deployment boundary | No customer-selected container image; production CDC is not enabled, according to handoff |

The assessed local ZIP is `/private/tmp/order-aggregator-legacy-trial/iceberg-kafka-connect-4ac3ebb15f72ae1fd3459623c17b5b14daadab4c-glue-1.1.27.zip`. Every JAR in the recovered `final-plugin/` directory was hashed and compared with the corresponding ZIP entry before scanning. The original workspace was preserved. S3 was not contacted to independently verify the object version.

### Source declarations are not the deployed versions

| Component | Checked-in build | Verified ZIP | Assessment consequence |
| --- | --- | --- | --- |
| Avro | `1.11.3` | `1.11.4` | CVE-2024-47561 applies to the source declaration, **not** the identified standalone Avro in this ZIP. Current Trivy and OSV checks returned no match for Avro 1.11.4. |
| Jackson | `2.14.2` | Standalone `2.17.2`, plus shaded core `2.13.4` / databind `2.13.4.2` in Parquet | Assess both copies. |
| Netty | Forced `4.1.100.Final` | `4.1.124.Final` | Findings must use the actual artifact version. |
| Hadoop shaded Guava | Forced `1.1.1-tabular` | Public `1.1.1`, embedding Guava `30.1.1-jre` | The recovered override changes both provenance and embedded dependency exposure. |
| AWS SDK | Iceberg AWS bundle in default distribution | Separate v2 modules `2.32.25`, plus v1 core/STS `1.12.782` | Both generations are present; they are not duplicate versions of the same namespace. |
| Kafka / SLF4J | Compile/test references, including Kafka `3.5.1` | Excluded from shipped runtime | Do not count source Kafka 3.5.1 as a deployed plugin dependency. |
| Other catalogs | Azure/GCP/Nessie; optional Hive distribution | Azure/GCP/Nessie bundles and Hive metastore omitted | Do not attribute the Hive distribution's CVEs to this baseline. ORC still embeds Hive storage API; that is distinct from shipping Hive metastore. |

Evidence: [`gradle/libs.versions.toml`](../../gradle/libs.versions.toml), [`kafka-connect-runtime/build.gradle`](../../kafka-connect-runtime/build.gradle), recovered init script, [resolved graph](evidence/baseline-gradle-dependencies.txt), and [JAR hashes](evidence/baseline-jar-sha256.csv).

## 2. Method and limits

- Ran **Trivy 0.76.1 `rootfs --scanners vuln`** on the checksum-verified extracted plugin. Vulnerability DB updated `2026-11-05T01:19:45.907162652Z`. Downloaded scanner archive SHA-256 was checked against GitHub release metadata.
- Used the deployed-filesystem mode because Trivy's source-filesystem mode did not identify binary JARs. The reported totals come from the successful JAR scan, not that empty preliminary scan.
- Inspected embedded Maven metadata, manifests, shaded class paths, duplicate classes, and selected decoder bytecode. Trivy returned 183 Java package records across 169 JAR paths, including embedded components. The three locally built connector JARs were covered by the hash inventory and targeted source/bytecode review, not by published-package advisory matching.
- Generated a Gradle runtime dependency report in a separate disposable checkout using a copy of the recovered cache and init script. This resolved the baseline graph offline; it did not build a replacement or run tests.
- Ran Trivy secret scanning on the current repository contents: **zero reported secret findings**. This does not cover Git history or prove absence of secrets. The visible `minioadmin` credentials are integration-test fixtures, not evidence of leaked production keys.
- Cross-checked OSV for Avro 1.11.4, Iceberg core 1.5.2, and upstream Kafka clients 3.7.0. No direct OSV matches for the first two do not negate transitive findings or manual code findings.
- Reviewed relevant Gradle configuration, CI workflow, control-message decoding, table routing, and security configuration inheritance. This is a targeted manual review, not a complete SAST or penetration test.
- Did **not** run Wiz, application tests, exploit payloads, AWS calls, GitHub checks, or a live infrastructure assessment. No OS/JVM binary scan, MSK platform scan, IAM/Lake Formation policy review, VPC assessment, or runtime reachability proof was performed.

**SBOM caveats:** the unmodified scanner output identifies `iceberg-bundled-guava-1.5.2.jar` as package version `1.5.0`, despite the artifact filename and resolved graph specifying 1.5.2. Retain the JAR hash and graph as the provenance record; the embedded Guava 33.0.0-jre was independently observed. The scanner-generated SBOM also omits published identities for the three locally built connector JARs. Use it together with the complete 172-JAR inventory. A release SBOM should reconcile these identities rather than silently accepting the scanner's inference.

Scanner counts can differ from Wiz because of advisory sources, package identification, CVSS versions, shaded-library detection, and deployment context. All raw matches, including low severity, are retained without suppression.

## 3. Dependency findings and recommended changes

The following groups account for **all 89 matches**. The complete per-match IDs, affected JARs, advisory fixed versions, titles, and links are in [baseline-findings.csv](evidence/baseline-findings.csv) and Appendix A. Upgrade versions below are advisory fix floors or candidate targets, **not a compatibility-tested dependency set**. Later releases still require a fresh scan.

Replace ☐ with ☑ in the **Fixed** column as each finding group is resolved.

| Fixed | ID / maximum severity | Confirmed components; matches | Exposure and practical interpretation | Recommended change |
| :---: | --- | --- | --- | --- |
| ☐ | D01 — Critical | `parquet-avro:1.13.1`; **2** | CVE-2025-30065 and CVE-2025-46762 concern schema processing while reading Parquet. The latter requires specific/reflect models; generic is not affected by that advisory. No affected reader call was identified in the repository's main source, but dependency internals and later uses are not exhaustively excluded. | Align the Parquet module family to **at least 1.15.2**, then address its shaded Jackson separately. Do not stop at 1.15.1. Verify Iceberg 1.5.2 writer/reader compatibility before adoption. |
| ☐ | D02 — Critical | Netty codec/http/http2/handler `4.1.124.Final`; **33** | CVE-2026-75595 requires a server with differing SNI/default client-certificate requirements. That topology is not evidenced here. Other matches involve response parsing, TLS trust-manager handling, decompression, and HTTP/2; outbound use alone does not dismiss all of them. | Replace the group-wide force with an aligned Netty **4.1.137.Final or later patched 4.1.x** set. Review all 33 entries; do not suppress the entire family based on the SNI finding's server prerequisite. |
| ☐ | D03 — High | Standalone Jackson core/databind `2.17.2`; **16** | Parser resource exhaustion and feature-specific databind issues. CVE-2026-54512/54513 require polymorphic typing/validator configurations; these were not identified in connector-owned ObjectMapper setup. Converter and dependency behavior still needs review. | Align standalone Jackson through a BOM to **2.18.11** or a later patched compatible 2.x line. Keep annotations/core/databind/dataformats consistent. Avoid a Jackson 3 migration as an untested stopgap. |
| ☐ | D04 — High | Jackson core `2.13.4` and databind `2.13.4.2` inside `parquet-jackson-1.13.1.jar`; **14** | Relocated classes are physically present under `shaded/parquet/com/fasterxml/jackson/`. Standalone Jackson constraints have no effect on these bytes. | Use a compatible Parquet build that embeds patched Jackson, or rebuild the owning shaded artifact with the patch. Verify its embedded classes and rescan. Even published Parquet **1.18.1** declares Jackson **2.22.2**, below the **2.22.3** fixes for several current findings; upgrading to that release alone is not a complete closure. |
| ☐ | D05 — High | Guava `32.0.1-jre` standalone, `30.1.1-jre` in Hadoop, `33.0.0-jre` in Iceberg; **5** | CVE-2026-102554 needs Java-object deserialization of affected Guava collections; no `ObjectInputStream` call was found in connector-owned main source. Relevant relocated collection classes are present. Hadoop's copy also has CVE-2023-2976/CVE-2020-8908 temporary-directory issues. | Upgrade standalone Guava to **33.7.2 or later patched release**. Independently replace/backport the embedded copies in `hadoop-shaded-guava` and `iceberg-bundled-guava`, preserving their relocation namespaces. A Maven constraint on `com.google.guava` does not repair them. If retaining Iceberg 1.5.2 prevents this, document a specific exception and deserialization-path evidence. |
| ☐ | D06 — High | `aircompressor:0.26`; **2** | CVE-2024-36114: JVM crash/memory disclosure from malformed compressed input. CVE-2025-67721: reused output-buffer disclosure. Ordinary compression/writing is not proof of the affected decompression path. | Evaluate **`io.airlift:aircompressor:2.0.3`**. Its POM targets Java 8 and the advisory lists this legacy-line fix. Merely moving to 0.27 fixes the older issue but not both. Do not blindly substitute the differently packaged/newer Java line. |
| ☐ | D07 — High | `at.yawk.lz4:lz4-java:1.10.2`; **6** | Includes CVE-2026-106451 native-library temporary-file race, which needs local shared-directory access and race conditions; other entries involve malformed compressed lengths and JNI validation. Managed-worker isolation was not inspected. | Upgrade this **actual Maven coordinate** to **1.11.4 or later patched release**. Review temporary-directory protections and native/Java implementation selection; configuration mitigations do not replace patching. |
| ☐ | D08 — High | `snappy-java:1.1.10.5`; **3** | Includes CVE-2026-93451 buffer overflow in typed uncompress-array methods, CVE-2026-108106 allocation DoS, and CVE-2026-116590 unsafe array-copy ranges. API-specific reachability is unproven. | Raise the explicit runtime force to **1.1.10.9 or later patched release** and verify native compatibility on the managed Linux worker. |
| ☐ | D09 — High | `wire-runtime-jvm:5.2.0`; **1** | CVE-2026-45799 concerns decoding malformed protobuf. The intended converter is Avro, but the serde dependency also supplies protobuf support; it was not proved unreachable throughout converter initialization and use. | Evaluate an aligned Wire **6.3.0** family or a supported backport. This is a major-version change; do not replace only one Wire JAR or remove protobuf/compiler dependencies without verifying converter behavior. |
| ☐ | D10 — High | `commons-beanutils:1.9.4`; **1** | CVE-2025-48734: attacker-controlled property paths can reach an enum's class loader. Presence comes through Hadoop; no connector-owned property-path API exposure was established. | Upgrade to **1.11.0+** on the 1.x line, or remove the owning unused dependency chain after linkage checks. |
| ☐ | D11 — High | `dnsjava:2.1.7`; **1** | CVE-2024-25638: DNS response validation issue. Its use in this Glue/S3 path is not established. | Evaluate **3.6.0+** or removal of the unused Hadoop resolver path. The 2.x-to-3.x change requires Hadoop compatibility review. |
| ☐ | D12 — High | HttpClient5 `5.3.1`, HttpCore5 and HttpCore5-H2 `5.2.4`; **3** | CVE-2026-64607 connection leak; CVE-2026-54399/54428 HTTP header/HPACK resource exhaustion. A Glue-only catalog reduces REST-catalog use, but these libraries are still shipped. | Use a compatible set containing **HttpClient5 5.6.3+** and **HttpCore5 / HttpCore5-H2 5.4.3+**. Validate the pair together. Keep these distinct from the SDK's separate HttpClient 4.x dependency. |
| ☐ | D13 — Medium | `commons-lang3:3.14.0`; **1** | CVE-2025-48924: excessive recursion in `ClassUtils.getClass` on very long input; relevant input path is not established. | Upgrade to **3.18.0+**. |
| ☐ | D14 — Low | `hadoop-common:3.3.6`; **1** | CVE-2024-23454 affects `RunJar.run()` temporary-directory permissions. MSK loads this as a connector plugin, not via an evidenced Hadoop RunJar invocation. | Upgrade to a compatible **3.4.0+** patched Hadoop line, remove Hadoop only after dependency analysis, or document a narrow RunJar-not-used exception. This does not fix shaded Guava by itself. |

### Dependency remediation cautions

- The source's `resolutionStrategy.force` and `eachDependency` rules can override a nominally newer dependency. Update the effective rules and inspect the resolved graph and final ZIP; editing only the version catalog is insufficient.
- The baseline already removes the unused Azure/GCP/Nessie bundles, Hive metastore, `iceberg-aws-bundle`, Kafka, SLF4J, and the old `javax.activation:activation` JAR. Preserve those exclusions.
- Keep AWS SDK v2 modules aligned under a BOM. Do not combine their resolved modules with `iceberg-aws-bundle`. The converter brings AWS SDK v1 STS dependencies; assess migration/removal through the converter's actual implementation, rather than assuming SDK v2 is a drop-in replacement.
- AWS SDK for Java v1 reached end of support on **2025-12-31** ([AWS announcement](https://aws.amazon.com/blogs/developer/announcing-end-of-support-for-aws-sdk-for-java-v1-x-on-december-31-2025/)). Treat the v1 dependency as a lifecycle risk, separately from scanner findings; no v1-specific CVE was reported by this scan.
- The current source declares Java 8 bytecode compatibility; the observed worker runs Java 17. It would be incorrect to label the deployed JVM as Java 8. Conversely, satisfying Java 17 requirements does not establish compatibility with Iceberg's APIs.
- Do not equate a library being unused in the intended flow with scanner clearance. Record a reviewed applicability decision for the exact CVE and artifact hash if retaining it.

## 4. Manual code, packaging, and CI findings

These are separate from the 89 scanner matches. Priorities below are reviewer assessments, not CVSS scores.

### S01 — High priority: control messages select classes before validation

**Evidence:** [`Channel.java`](../../kafka-connect/src/main/java/io/tabular/iceberg/connect/channel/Channel.java), lines 122–139, decodes each control-topic value before comparing `event.groupId()`. [`EventDecoder.java`](../../kafka-connect/src/main/java/io/tabular/iceberg/connect/channel/EventDecoder.java), lines 71–77, calls Iceberg's decoder and retains a legacy fallback. [`DeprecatedAvroEncoderUtil.java`](../../kafka-connect-events/src/main/java/org/apache/iceberg/avro/DeprecatedAvroEncoderUtil.java), lines 86–93, parses the schema embedded in the message. [`DeprecatedGenericAvroReader.java`](../../kafka-connect-events/src/main/java/org/apache/iceberg/avro/DeprecatedGenericAvroReader.java), lines 91–96, uses its record name for class lookup before checking `IndexedRecord` assignability.

Bytecode inspection of the shipped Iceberg 1.5.2 classes shows the current decoder also routes schema record names into `GenericAvroReader$ResolvingReadBuilder.recordReader`. `DynClasses$Builder.impl` calls `Class.forName(name, true, loader)`: class initialization can occur before type validation. See [decoder bytecode evidence](evidence/decoder-bytecode.txt).

**Impact/condition:** someone who can write to the connector's control topic can submit schemas that select available classes for initialization, or malformed events that fail decoding. The group ID is not an authentication boundary. A useful code-execution gadget and a working exploit were **not** demonstrated. Decoder exceptions can also reach the coordinator loop, whose exception handler stops the coordinator thread (`CoordinatorThread.java`, lines 36–44).

**Recommended change:** treat the control topic as a privileged channel. Restrict read/write/group/transactional permissions to the intended connector principals through the actual MSK authorization mechanism; ordinary source-topic producers should not write control events. Review AWS IAM `kafka-cluster` permissions for IAM-authenticated clients, and broker ACLs where another authorization mechanism applies. Validate schema names, field IDs, sizes and nesting against the expected protocol before any class lookup; use explicit permitted-class mappings or a reader without arbitrary class initialization. Cover both current and legacy decoders. Reject and alert on invalid control events with a recovery policy that preserves commit correctness; do not silently skip them.

This remains relevant despite Avro 1.11.4 fixing CVE-2024-47561. An Avro upgrade alone does not remove the custom/Iceberg class-loading code. Do not remove the legacy decoder or either events JAR until control-topic compatibility is proven.

### S02 — Conditional high priority: routing and CDC extend producer authority

**Evidence:** [`Worker.java`](../../kafka-connect/src/main/java/io/tabular/iceberg/connect/channel/Worker.java), lines 135–157, accepts a record field as a table name when dynamic routing is enabled and caches writers without an explicit cardinality limit. [`IcebergWriterFactory.java`](../../kafka-connect/src/main/java/io/tabular/iceberg/connect/data/IcebergWriterFactory.java), lines 49–65, loads that table or can create it. [`IcebergWriter.java`](../../kafka-connect/src/main/java/io/tabular/iceberg/connect/data/IcebergWriter.java), lines 70–74 and 109–129, acts on record-supplied CDC operations.

**Impact/condition:** dynamic routing with a broadly privileged execution role can allow a topic producer to direct writes to additional accessible tables and grow resource use through many targets. Enabling CDC intentionally permits delete/update operations from input. These are trust-boundary concerns, not evidence that the current production configuration enables them. Dynamic routing and automatic table creation default to false; production CDC is reported disabled.

**Recommended change:** use an explicit table allowlist/static routes for the stopgap, scope Glue/Lake Formation and S3 access to the intended tables/prefixes, and limit routing cardinality if dynamic routing is required. Restrict source-topic writers and schema publishers. Enable auto-create/schema evolution/CDC only for the approved use case; preserve the tested CDC behavior during later remediation, but do not enable it in production as part of packaging work.

### S03 — High priority: external packaging inputs and incomplete reproducibility

**Evidence:** the working recipe is an external init script plus three thin Glue JARs and their embedded POMs from an earlier plugin. The checked-in build has neither this recipe nor dependency lockfiles/verification metadata. [`build.gradle`](../../build.gradle), lines 23–27, resolves through Maven Central and a Tabular S3 repository; the original `1.1.1-tabular` dependency needed replacement. Build timestamps/JDK/OS are embedded in JAR manifests at lines 37–46.

**Impact:** rebuilding the pinned source without the recovered inputs can ship a different, more vulnerable classpath or fail. A converter fat JAR with the same Maven version is not equivalent to the verified thin input. A Git pin alone is not artifact integrity or reproducible dependency resolution.

**Recommended change:** check in a dedicated Glue/MSK packaging profile reproducing the known exclusions and dependency additions. Preserve the three exact Glue inputs by hash in a controlled immutable artifact repository, including their matching embedded POMs. Scope repository resolution to those exact inputs; restrict the legacy Tabular repository to necessary artifacts. Commit dependency locks, reviewed SHA-256 verification metadata, and an SBOM/hash manifest for the resulting distribution. Pin the build JDK and Gradle distribution checksum. If byte-for-byte reproducibility is required, normalize archive order/timestamps and remove or deterministically derive build-time manifest fields; otherwise describe the result as dependency-reproducible rather than byte-reproducible.

Preserve these donor input hashes:

| Thin Glue JAR | SHA-256 |
| --- | --- |
| `schema-registry-common-1.1.27.jar` | `8a589d6a6afecbd9aafa08bcfe9751781f3abcce8420910abdc729316db9076a` |
| `schema-registry-kafkaconnect-converter-1.1.27.jar` | `11442279f534209ea46c853dcd5a609eab81686545e12b85f2b72d8150e3580e` |
| `schema-registry-serde-1.1.27.jar` | `2093abc6519aa57edb79535c7ebe18cd3d2d029af3da65fe03bf14c72abfd957` |

### S04 — Medium priority: CI and wrapper supply-chain hardening

**Evidence:** [CI workflow](../../.github/workflows/ci-build.yaml), lines 3–6, grants workflow-wide `id-token: write`; the displayed build has no OIDC-consuming step. Actions are referenced by mutable tags at lines 43, 48, and 51. Workflow-only changes are excluded from push/PR triggering at lines 14–20. [Wrapper properties](../../gradle/wrapper/gradle-wrapper.properties) specify HTTPS Gradle 8.1.1 but no `distributionSha256Sum`.

**Recommended change:** remove unused OIDC issuance permission or confine it to a job that actually needs it; pin reviewed action commits; validate workflow changes; add the independently verified Gradle distribution SHA-256 and maintain the wrapper-validation step. Review a supported Gradle/action upgrade separately. No specific Gradle CVE or compromised workflow was established here.

### S05 — High priority: unmaintained upstream and exception ownership

The [repository README](../../README.md) explicitly says this repository is no longer maintained and redirects to Apache Iceberg. Temporary use needs a named internal owner for dependency/security patches, an expiry date, and a migration milestone. Iceberg 1.5.2 and the custom converter packaging must not become an indefinite exception simply because the initial trial worked.

### S06 — Deployment verification: control-client security settings

[`IcebergSinkConfig.java`](../../kafka-connect/src/main/java/io/tabular/iceberg/connect/IcebergSinkConfig.java), lines 252–260 and 454–475, attempts to load worker properties from the Java command line and otherwise uses only explicit `iceberg.kafka.*` settings. [`KafkaClientFactory.java`](../../kafka-connect/src/main/java/io/tabular/iceberg/connect/channel/KafkaClientFactory.java) creates separate producer, consumer, and admin clients from those properties.

Verify the effective control-client authentication, TLS, hostname verification and bootstrap endpoints on MSK Connect; do not assume worker-property auto-discovery always succeeds on a managed service. Preserve worker-provided IAM authentication and use its supported configuration. This review did **not** find evidence that the deployed control client uses plaintext.

## 5. AWS-managed worker findings: separate ownership

OSV's query for upstream `org.apache.kafka:kafka-clients:3.7.0` returned the following four advisories. These are **version-based candidates**, not confirmed vulnerabilities in AWS's patched managed runtime, and are **not included in the 89 plugin matches**.

| Advisory | Upstream issue / condition | Upstream fix information and action |
| --- | --- | --- |
| [CVE-2024-31141](https://nvd.nist.gov/vuln/detail/CVE-2024-31141) | Automatic ConfigProvider filesystem/environment access when an untrusted party controls client configuration | OSV records a 3.7.1 fixed boundary; advisory guidance also recommends 3.8.0+ with `org.apache.kafka.automatic.config.providers=none` where appropriate, plus provider path restrictions. Ask AWS which fix/control exists in its worker. |
| [CVE-2025-27817](https://nvd.nist.gov/vuln/detail/CVE-2025-27817) | File read / SSRF through attacker-controlled SASL/OAUTHBEARER URLs | Upstream 3.9.1 introduces URL allowlisting, permissive by default in that branch; 4.0 changes the default. Intended AWS IAM authentication is a different mechanism, but configuration permissions still matter. |
| [CVE-2026-35554](https://nvd.nist.gov/vuln/detail/CVE-2026-35554) | Producer buffer-pool race can corrupt or misroute messages when a batch expires with a request in flight | Fixed upstream in 3.9.2 / 4.0.2 / 4.1.2. This is relevant to integrity review because the connector uses a Kafka producer for control messages. Confirm AWS backport status. |
| [CVE-2026-33558](https://nvd.nist.gov/vuln/detail/CVE-2026-33558) | Sensitive Kafka protocol data in DEBUG logging | Fixed upstream in 3.9.2 / 4.0.1. Keep relevant network/client logging at INFO and restrict log access while AWS confirms the worker patch level. |

Keep Kafka/Connect, SLF4J, and MSK IAM libraries out of the plugin as in the tested baseline. Adding replacement Kafka JARs is not an acceptable way to patch the managed worker and can break class loading. Request AWS's vulnerability/backport statement and supported upgrade path for the exact MSK Connect runtime. Ask for the corresponding Corretto/JVM and host patch posture; a Java version string alone is not a complete OS/JVM assessment.

## 6. Recommended remediation sequence

1. **Freeze the evidence.** Retain the original ZIP, S3 VersionId, SHA-256, build inputs, recipe, worker logs, and this assessment. Do not replace the object in place.
2. **Make the packaging repeatable.** Implement S03 using the known standard distribution and thin Glue inputs. Capture a resolved lock/verification set before making dependency changes, then review the dependency diff.
3. **Patch standalone families.** Evaluate the D02/D03/D06–D13 targets in small, reviewable groups. Update effective force rules. Preserve SDK alignment, classloader exclusions, Glue/S3 clients and converter role behavior.
4. **Repair owning shaded artifacts.** Align Parquet and address its embedded Jackson. Separately repair the Iceberg and Hadoop relocated Guava copies. Verify embedded metadata and actual class contents after each change; fresh standalone dependencies are insufficient.
5. **Address code and configuration exposure.** Implement or formally accept the control-decoder risk, confirm privileged control-topic authorization, static table scope, source/schema publisher permissions, TLS settings, and AWS-owned patches.
6. **Validate a candidate before release.** When separately authorized, run compatibility/security checks on the candidate: converter schema decoding and tombstones; connector/transforms loading; Glue/Lake Formation and S3 access; decimal/timestamp/null values; format-v2 inserts/updates/deletes; routing; restart/rebalance and offset continuity; old/new control-message handling; native codecs on the managed Linux worker. The historical probe used Kafka 3.7.1, not the observed worker's 3.7.0, so it is not sufficient evidence for the new dependency set.
7. **Rescan the exact candidate ZIP**, including relocated classes, and reconcile findings/identities in its SBOM. Publish only after approval, under a new immutable artifact key, recording the new SHA-256 and S3 VersionId.

Runtime behavior to preserve:

- `io.tabular.iceberg.connect.IcebergSinkConnector`, `DebeziumTransform`, `_cdc.op`, `_cdc.target`, and the tested target-name pattern.
- `GlueCatalog` and `S3FileIO`, format-v2 table semantics, and Lake Formation access.
- `com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter`: only the converter assumes the schema-registry reader role; catalog/S3 use the connector execution role.
- Existing source consumer-group identity and offsets. The prior `TableReference.get(): Unknown field ordinal: 3` incident means control-topic migration needs an explicit compatibility/cutover plan; changing topics casually is not a security fix.
- Both events JARs until proven otherwise: legacy classes use `io.tabular.iceberg.connect.events`, while the Apache events JAR uses `org.apache.iceberg.connect.events`. They are not interchangeable duplicate artifacts.

## 7. Conditions for an unchanged-artifact stopgap exception

These are required evidence requests for the proposed exception, **not assertions that controls are already configured**.

| Required evidence / control | Proposed owner |
| --- | --- |
| Exact artifact hash, finite expiry date, named risk owner, and replacement milestone; scheduled rescans and a procedure for newly disclosed critical issues | Security + service owner |
| Per-CVE dispositions for retained findings: affected component/hash, exploit prerequisite, evidence supporting any not-affected claim, compensating control, owner and expiry. Do not suppress by package family. | Security + connector maintainer |
| Dedicated, authenticated control topic; connector-only write access; tightly scoped group/transactional permissions and change audit | Kafka/MSK platform owner |
| Least-privilege execution role for intended Glue/Lake Formation tables, S3 prefixes and KMS keys; narrowly scoped converter reader-role trust; no static access keys in connector configuration | AWS/IAM + data platform owners |
| Restricted source-topic writers and schema publishers; static permitted table set; no unapproved auto-create, schema evolution or production CDC enablement | Data platform owner |
| Restricted connector/plugin/worker configuration changes, access to the immutable artifact bucket, and artifact substitution controls | Platform owner |
| Effective TLS/hostname validation and supported MSK IAM setup for source and control clients; appropriately restricted AWS endpoint/network access | Platform owner |
| AWS response for Kafka/JVM/host patch status; INFO-level sensitive network logging and controlled CloudWatch access | Platform + AWS support |
| Alerts on failed tasks/coordinator shutdown, consumer lag/commit stalls, unexpected table activity, schema changes and malformed messages; approved rollback/cutover that preserves offsets | Service owner |

Do not consider deployment approved if control-topic access, artifact identity, or the retained critical/high risk dispositions remain unowned. The historical dev/CDC evidence supports functionality of that trial only; production CDC remains disabled unless separately approved.

## 8. Evidence and reproducibility

| File | Purpose |
| --- | --- |
| [baseline-trivy.json](evidence/baseline-trivy.json) | Unmodified scanner package and vulnerability results, severities, CVSS vectors, descriptions and references |
| [baseline-findings.csv](evidence/baseline-findings.csv) | All 89 matches for security-team triage |
| [baseline.cdx.json](evidence/baseline.cdx.json) | Scanner-generated CycloneDX SBOM; use with the identification caveats in section 2 |
| [baseline-jar-sha256.csv](evidence/baseline-jar-sha256.csv) | Complete 172-JAR inventory with ZIP entry, size and SHA-256 |
| [baseline-gradle-dependencies.txt](evidence/baseline-gradle-dependencies.txt) | Baseline resolved runtime graph using recovered packaging inputs |
| [decoder-bytecode.txt](evidence/decoder-bytecode.txt) | Read-only bytecode inspection supporting S01 and converter-class review |
| [source-secrets-trivy.json](evidence/source-secrets-trivy.json) | Repository secret-scan result; no findings reported |
| [osv-kafka-clients-3.7.0.json](evidence/osv-kafka-clients-3.7.0.json) | Worker-version advisory candidates and upstream fix information |
| [osv-avro-1.11.4.json](evidence/osv-avro-1.11.4.json), [osv-iceberg-core-1.5.2.json](evidence/osv-iceberg-core-1.5.2.json) | OSV query results; empty responses mean no returned match, not a security guarantee |
| [assessment-metadata.json](evidence/assessment-metadata.json) | Scanner/database identity, exact scan command, scope and evidence checksums |

After verifying and extracting an exact candidate ZIP into a dedicated directory, a comparable scan is:

```bash
trivy rootfs --scanners vuln --disable-telemetry --skip-version-check \
  --format json --output plugin-trivy.json /path/to/extracted-plugin
trivy convert --format cyclonedx --output plugin.cdx.json plugin-trivy.json
```

Capture scanner version, DB timestamps, ZIP and JAR hashes each time. A future run against a refreshed database can legitimately produce different counts. Never interpret an empty source-only scan as a successful binary dependency scan; inspect the identified packages.

### Additional triage notes

- The final ZIP has **316 duplicate class paths**, all between Parquet 1.13.1 JARs. This is a classpath-review item, not 316 vulnerabilities. The earlier activation duplicate report is not evidence against the final ZIP.
- The converter contains `org/apache/avro/Schemas.class`; no second copy of that class was found in this ZIP. Bytecode shows a helper using Avro's package-private schema internals. It is a compatibility concern for future Avro upgrades, not evidence that Avro 1.11.3 is secretly bundled.
- No Log4Shell finding, Commons Text interpolation CVE, production hardcoded secret, or exposed container setting is asserted merely because a related project or old dependency exists. Test HTTP endpoints, MinIO credentials and test-container versions are not the managed deployment's configuration.
- Advisory references are preserved per finding. Relevant primary/vendor references include [Parquet schema advisory](https://lists.apache.org/thread/okzqb3kn479gqzxm21gg5vqr35om9gw5), [Parquet follow-up advisory](https://lists.apache.org/thread/t7724lpvl110xsbgqwsmrdsns0rhycdp), [Netty SNI advisory](https://github.com/netty/netty/security/advisories/GHSA-c4c3-7fpv-j4q5), [Guava deserialization advisory](https://github.com/google/guava/security/advisories/GHSA-xxph-c9ww-hj94), [Aircompressor advisory](https://github.com/airlift/aircompressor/security/advisories/GHSA-vx9q-rhv9-3jvg), and the [published Parquet 1.18.1 POM](https://repo.maven.apache.org/maven2/org/apache/parquet/parquet/1.18.1/parquet-1.18.1.pom).

## Appendix A. Complete package/advisory match register

Each row is a scanner match, not a confirmed exploit. The same CVE can occur in several JARs. Fixed versions are copied from scanner metadata, may describe multiple release branches, and do not constitute a tested upgrade combination. Use the D01–D14 recommendations for family alignment, shading and compatibility constraints. All paths below are relative to the distribution's `lib/` directory.

| Group | Severity | CVE | Component version | JAR | Advisory fixed versions |
| --- | --- | --- | --- | --- | --- |
| D01 | CRITICAL | [CVE-2025-30065](https://avd.aquasec.com/nvd/cve-2025-30065) | `org.apache.parquet:parquet-avro:1.13.1` | `parquet-avro-1.13.1.jar` | 1.15.1 |
| D01 | HIGH | [CVE-2025-46762](https://avd.aquasec.com/nvd/cve-2025-46762) | `org.apache.parquet:parquet-avro:1.13.1` | `parquet-avro-1.13.1.jar` | 1.15.2 |
| D02 | MEDIUM | [CVE-2025-58057](https://avd.aquasec.com/nvd/cve-2025-58057) | `io.netty:netty-codec:4.1.124.Final` | `netty-codec-4.1.124.Final.jar` | 4.1.125.Final |
| D02 | HIGH | [CVE-2026-42583](https://avd.aquasec.com/nvd/cve-2026-42583) | `io.netty:netty-codec:4.1.124.Final` | `netty-codec-4.1.124.Final.jar` | 4.1.133.Final |
| D02 | HIGH | [CVE-2026-59901](https://avd.aquasec.com/nvd/cve-2026-59901) | `io.netty:netty-codec:4.1.124.Final` | `netty-codec-4.1.124.Final.jar` | 4.1.136.Final |
| D02 | LOW | [CVE-2025-58056](https://avd.aquasec.com/nvd/cve-2025-58056) | `io.netty:netty-codec-http:4.1.124.Final` | `netty-codec-http-4.1.124.Final.jar` | 4.1.125.Final, 4.2.5.Final |
| D02 | MEDIUM | [CVE-2025-67735](https://avd.aquasec.com/nvd/cve-2025-67735) | `io.netty:netty-codec-http:4.1.124.Final` | `netty-codec-http-4.1.124.Final.jar` | 4.2.8.Final, 4.1.129.Final |
| D02 | HIGH | [CVE-2026-33870](https://avd.aquasec.com/nvd/cve-2026-33870) | `io.netty:netty-codec-http:4.1.124.Final` | `netty-codec-http-4.1.124.Final.jar` | 4.1.132.Final, 4.2.10.Final |
| D02 | MEDIUM | [CVE-2026-41417](https://avd.aquasec.com/nvd/cve-2026-41417) | `io.netty:netty-codec-http:4.1.124.Final` | `netty-codec-http-4.1.124.Final.jar` | 4.1.133.Final, 4.2.13.Final |
| D02 | MEDIUM | [CVE-2026-42580](https://avd.aquasec.com/nvd/cve-2026-42580) | `io.netty:netty-codec-http:4.1.124.Final` | `netty-codec-http-4.1.124.Final.jar` | 4.2.13.Final, 4.1.133.Final |
| D02 | MEDIUM | [CVE-2026-42581](https://avd.aquasec.com/nvd/cve-2026-42581) | `io.netty:netty-codec-http:4.1.124.Final` | `netty-codec-http-4.1.124.Final.jar` | 4.2.13.Final, 4.1.133.Final |
| D02 | HIGH | [CVE-2026-42584](https://avd.aquasec.com/nvd/cve-2026-42584) | `io.netty:netty-codec-http:4.1.124.Final` | `netty-codec-http-4.1.124.Final.jar` | 4.2.13.Final, 4.1.133.Final |
| D02 | MEDIUM | [CVE-2026-42585](https://avd.aquasec.com/nvd/cve-2026-42585) | `io.netty:netty-codec-http:4.1.124.Final` | `netty-codec-http-4.1.124.Final.jar` | 4.2.13.Final, 4.1.133.Final |
| D02 | HIGH | [CVE-2026-42587](https://avd.aquasec.com/nvd/cve-2026-42587) | `io.netty:netty-codec-http:4.1.124.Final` | `netty-codec-http-4.1.124.Final.jar` | 4.2.13.Final, 4.1.133.Final |
| D02 | MEDIUM | [CVE-2026-50020](https://avd.aquasec.com/nvd/cve-2026-50020) | `io.netty:netty-codec-http:4.1.124.Final` | `netty-codec-http-4.1.124.Final.jar` | 4.2.15.Final, 4.1.135.Final |
| D02 | HIGH | [CVE-2026-55831](https://avd.aquasec.com/nvd/cve-2026-55831) | `io.netty:netty-codec-http:4.1.124.Final` | `netty-codec-http-4.1.124.Final.jar` | 4.2.16.Final, 4.1.136.Final |
| D02 | HIGH | [CVE-2026-55833](https://avd.aquasec.com/nvd/cve-2026-55833) | `io.netty:netty-codec-http:4.1.124.Final` | `netty-codec-http-4.1.124.Final.jar` | 4.2.16.Final, 4.1.136.Final |
| D02 | HIGH | [CVE-2026-56745](https://avd.aquasec.com/nvd/cve-2026-56745) | `io.netty:netty-codec-http:4.1.124.Final` | `netty-codec-http-4.1.124.Final.jar` | 4.2.16.Final, 4.1.136.Final |
| D02 | MEDIUM | [CVE-2026-56746](https://avd.aquasec.com/nvd/cve-2026-56746) | `io.netty:netty-codec-http:4.1.124.Final` | `netty-codec-http-4.1.124.Final.jar` | 4.2.16.Final, 4.1.136.Final |
| D02 | MEDIUM | [CVE-2026-59898](https://avd.aquasec.com/nvd/cve-2026-59898) | `io.netty:netty-codec-http:4.1.124.Final` | `netty-codec-http-4.1.124.Final.jar` | 4.2.16.Final, 4.1.136.Final |
| D02 | MEDIUM | [CVE-2026-59899](https://avd.aquasec.com/nvd/cve-2026-59899) | `io.netty:netty-codec-http:4.1.124.Final` | `netty-codec-http-4.1.124.Final.jar` | 4.2.16.Final, 4.1.136.Final |
| D02 | MEDIUM | [CVE-2026-59903](https://avd.aquasec.com/nvd/cve-2026-59903) | `io.netty:netty-codec-http:4.1.124.Final` | `netty-codec-http-4.1.124.Final.jar` | 4.2.17.Final, 4.1.137.Final |
| D02 | MEDIUM | [CVE-2026-59921](https://avd.aquasec.com/nvd/cve-2026-59921) | `io.netty:netty-codec-http:4.1.124.Final` | `netty-codec-http-4.1.124.Final.jar` | 4.2.16.Final, 4.1.136.Final |
| D02 | HIGH | [CVE-2026-33871](https://avd.aquasec.com/nvd/cve-2026-33871) | `io.netty:netty-codec-http2:4.1.124.Final` | `netty-codec-http2-4.1.124.Final.jar` | 4.1.132.Final, 4.2.11.Final |
| D02 | HIGH | [CVE-2026-42587](https://avd.aquasec.com/nvd/cve-2026-42587) | `io.netty:netty-codec-http2:4.1.124.Final` | `netty-codec-http2-4.1.124.Final.jar` | 4.2.13.Final, 4.1.133.Final |
| D02 | MEDIUM | [CVE-2026-47244](https://avd.aquasec.com/nvd/cve-2026-47244) | `io.netty:netty-codec-http2:4.1.124.Final` | `netty-codec-http2-4.1.124.Final.jar` | 4.2.15.Final, 4.1.135.Final |
| D02 | MEDIUM | [CVE-2026-48043](https://avd.aquasec.com/nvd/cve-2026-48043) | `io.netty:netty-codec-http2:4.1.124.Final` | `netty-codec-http2-4.1.124.Final.jar` | 4.1.135.Final, 4.2.15.Final |
| D02 | MEDIUM | [CVE-2026-50560](https://avd.aquasec.com/nvd/cve-2026-50560) | `io.netty:netty-codec-http2:4.1.124.Final` | `netty-codec-http2-4.1.124.Final.jar` | 4.2.15.Final, 4.1.135.Final |
| D02 | HIGH | [CVE-2026-56819](https://avd.aquasec.com/nvd/cve-2026-56819) | `io.netty:netty-codec-http2:4.1.124.Final` | `netty-codec-http2-4.1.124.Final.jar` | 4.2.16.Final, 4.1.136.Final |
| D02 | MEDIUM | [CVE-2026-59900](https://avd.aquasec.com/nvd/cve-2026-59900) | `io.netty:netty-codec-http2:4.1.124.Final` | `netty-codec-http2-4.1.124.Final.jar` | 4.2.16.Final, 4.1.136.Final |
| D02 | HIGH | [CVE-2026-44249](https://avd.aquasec.com/nvd/cve-2026-44249) | `io.netty:netty-handler:4.1.124.Final` | `netty-handler-4.1.124.Final.jar` | 4.2.15.Final, 4.1.135.Final |
| D02 | HIGH | [CVE-2026-45416](https://avd.aquasec.com/nvd/cve-2026-45416) | `io.netty:netty-handler:4.1.124.Final` | `netty-handler-4.1.124.Final.jar` | 4.2.15.Final, 4.1.135.Final |
| D02 | HIGH | [CVE-2026-50010](https://avd.aquasec.com/nvd/cve-2026-50010) | `io.netty:netty-handler:4.1.124.Final` | `netty-handler-4.1.124.Final.jar` | 4.2.15.Final, 4.1.135.Final |
| D02 | CRITICAL | [CVE-2026-75595](https://avd.aquasec.com/nvd/cve-2026-75595) | `io.netty:netty-handler:4.1.124.Final` | `netty-handler-4.1.124.Final.jar` | 4.2.17.Final, 4.1.137.Final |
| D02 | MEDIUM | [CVE-2026-75596](https://avd.aquasec.com/nvd/cve-2026-75596) | `io.netty:netty-handler:4.1.124.Final` | `netty-handler-4.1.124.Final.jar` | 4.2.17.Final, 4.1.137.Final |
| D03 | MEDIUM | [CVE-2026-18401](https://avd.aquasec.com/nvd/cve-2026-18401) | `com.fasterxml.jackson.core:jackson-core:2.17.2` | `jackson-core-2.17.2.jar` | 2.21.1, 2.18.6 |
| D03 | HIGH | [CVE-2026-68494](https://avd.aquasec.com/nvd/cve-2026-68494) | `com.fasterxml.jackson.core:jackson-core:2.17.2` | `jackson-core-2.17.2.jar` | 2.18.8, 2.21.4 |
| D03 | HIGH | [CVE-2026-89407](https://avd.aquasec.com/nvd/cve-2026-89407) | `com.fasterxml.jackson.core:jackson-core:2.17.2` | `jackson-core-2.17.2.jar` | 2.18.11, 2.21.7, 2.22.3 |
| D03 | HIGH | [CVE-2026-89425](https://avd.aquasec.com/nvd/cve-2026-89425) | `com.fasterxml.jackson.core:jackson-core:2.17.2` | `jackson-core-2.17.2.jar` | 2.21.7, 2.22.3, 2.18.11 |
| D03 | MEDIUM | [CVE-2026-19032](https://avd.aquasec.com/nvd/cve-2026-19032) | `com.fasterxml.jackson.core:jackson-databind:2.17.2` | `jackson-databind-2.17.2.jar` | 2.18.10, 2.21.6, 2.22.2 |
| D03 | HIGH | [CVE-2026-54512](https://avd.aquasec.com/nvd/cve-2026-54512) | `com.fasterxml.jackson.core:jackson-databind:2.17.2` | `jackson-databind-2.17.2.jar` | 2.18.8, 3.1.4, 2.21.4 |
| D03 | HIGH | [CVE-2026-54513](https://avd.aquasec.com/nvd/cve-2026-54513) | `com.fasterxml.jackson.core:jackson-databind:2.17.2` | `jackson-databind-2.17.2.jar` | 2.18.8, 2.21.4, 3.1.4 |
| D03 | MEDIUM | [CVE-2026-54514](https://avd.aquasec.com/nvd/cve-2026-54514) | `com.fasterxml.jackson.core:jackson-databind:2.17.2` | `jackson-databind-2.17.2.jar` | 2.18.8, 2.21.4, 3.1.4 |
| D03 | MEDIUM | [CVE-2026-54515](https://avd.aquasec.com/nvd/cve-2026-54515) | `com.fasterxml.jackson.core:jackson-databind:2.17.2` | `jackson-databind-2.17.2.jar` | 3.1.4, 2.18.9, 2.21.5, 2.22.1 |
| D03 | MEDIUM | [CVE-2026-59888](https://avd.aquasec.com/nvd/cve-2026-59888) | `com.fasterxml.jackson.core:jackson-databind:2.17.2` | `jackson-databind-2.17.2.jar` | 2.18.8, 2.21.4 |
| D03 | MEDIUM | [CVE-2026-59889](https://avd.aquasec.com/nvd/cve-2026-59889) | `com.fasterxml.jackson.core:jackson-databind:2.17.2` | `jackson-databind-2.17.2.jar` | 2.21.5, 2.18.9, 2.22.1 |
| D03 | HIGH | [CVE-2026-68497](https://avd.aquasec.com/nvd/cve-2026-68497) | `com.fasterxml.jackson.core:jackson-databind:2.17.2` | `jackson-databind-2.17.2.jar` | 2.18.10, 2.21.6, 2.22.2 |
| D03 | MEDIUM | [CVE-2026-77310](https://avd.aquasec.com/nvd/cve-2026-77310) | `com.fasterxml.jackson.core:jackson-databind:2.17.2` | `jackson-databind-2.17.2.jar` | 2.18.9, 2.21.5, 2.22.1 |
| D03 | MEDIUM | [CVE-2026-83557](https://avd.aquasec.com/nvd/cve-2026-83557) | `com.fasterxml.jackson.core:jackson-databind:2.17.2` | `jackson-databind-2.17.2.jar` | 2.18.10, 2.21.6, 2.22.2 |
| D03 | HIGH | [CVE-2026-91776](https://avd.aquasec.com/nvd/cve-2026-91776) | `com.fasterxml.jackson.core:jackson-databind:2.17.2` | `jackson-databind-2.17.2.jar` | 2.18.11, 2.21.7, 2.22.3 |
| D03 | HIGH | [CVE-2026-91777](https://avd.aquasec.com/nvd/cve-2026-91777) | `com.fasterxml.jackson.core:jackson-databind:2.17.2` | `jackson-databind-2.17.2.jar` | 2.21.7, 2.18.11, 2.22.3 |
| D04 | HIGH | [CVE-2025-52999](https://avd.aquasec.com/nvd/cve-2025-52999) | `com.fasterxml.jackson.core:jackson-core:2.13.4` | `parquet-jackson-1.13.1.jar` | 2.15.0 |
| D04 | HIGH | [CVE-2026-68494](https://avd.aquasec.com/nvd/cve-2026-68494) | `com.fasterxml.jackson.core:jackson-core:2.13.4` | `parquet-jackson-1.13.1.jar` | 2.18.8, 2.21.4 |
| D04 | HIGH | [CVE-2026-89425](https://avd.aquasec.com/nvd/cve-2026-89425) | `com.fasterxml.jackson.core:jackson-core:2.13.4` | `parquet-jackson-1.13.1.jar` | 2.21.7, 2.22.3, 2.18.11 |
| D04 | MEDIUM | [CVE-2026-19032](https://avd.aquasec.com/nvd/cve-2026-19032) | `com.fasterxml.jackson.core:jackson-databind:2.13.4.2` | `parquet-jackson-1.13.1.jar` | 2.18.10, 2.21.6, 2.22.2 |
| D04 | MEDIUM | [CVE-2026-50193](https://avd.aquasec.com/nvd/cve-2026-50193) | `com.fasterxml.jackson.core:jackson-databind:2.13.4.2` | `parquet-jackson-1.13.1.jar` | 2.14.0 |
| D04 | HIGH | [CVE-2026-54512](https://avd.aquasec.com/nvd/cve-2026-54512) | `com.fasterxml.jackson.core:jackson-databind:2.13.4.2` | `parquet-jackson-1.13.1.jar` | 2.18.8, 3.1.4, 2.21.4 |
| D04 | HIGH | [CVE-2026-54513](https://avd.aquasec.com/nvd/cve-2026-54513) | `com.fasterxml.jackson.core:jackson-databind:2.13.4.2` | `parquet-jackson-1.13.1.jar` | 2.18.8, 2.21.4, 3.1.4 |
| D04 | MEDIUM | [CVE-2026-54514](https://avd.aquasec.com/nvd/cve-2026-54514) | `com.fasterxml.jackson.core:jackson-databind:2.13.4.2` | `parquet-jackson-1.13.1.jar` | 2.18.8, 2.21.4, 3.1.4 |
| D04 | MEDIUM | [CVE-2026-54515](https://avd.aquasec.com/nvd/cve-2026-54515) | `com.fasterxml.jackson.core:jackson-databind:2.13.4.2` | `parquet-jackson-1.13.1.jar` | 3.1.4, 2.18.9, 2.21.5, 2.22.1 |
| D04 | MEDIUM | [CVE-2026-59889](https://avd.aquasec.com/nvd/cve-2026-59889) | `com.fasterxml.jackson.core:jackson-databind:2.13.4.2` | `parquet-jackson-1.13.1.jar` | 2.21.5, 2.18.9, 2.22.1 |
| D04 | MEDIUM | [CVE-2026-77310](https://avd.aquasec.com/nvd/cve-2026-77310) | `com.fasterxml.jackson.core:jackson-databind:2.13.4.2` | `parquet-jackson-1.13.1.jar` | 2.18.9, 2.21.5, 2.22.1 |
| D04 | MEDIUM | [CVE-2026-83557](https://avd.aquasec.com/nvd/cve-2026-83557) | `com.fasterxml.jackson.core:jackson-databind:2.13.4.2` | `parquet-jackson-1.13.1.jar` | 2.18.10, 2.21.6, 2.22.2 |
| D04 | HIGH | [CVE-2026-91776](https://avd.aquasec.com/nvd/cve-2026-91776) | `com.fasterxml.jackson.core:jackson-databind:2.13.4.2` | `parquet-jackson-1.13.1.jar` | 2.18.11, 2.21.7, 2.22.3 |
| D04 | HIGH | [CVE-2026-91777](https://avd.aquasec.com/nvd/cve-2026-91777) | `com.fasterxml.jackson.core:jackson-databind:2.13.4.2` | `parquet-jackson-1.13.1.jar` | 2.21.7, 2.18.11, 2.22.3 |
| D05 | HIGH | [CVE-2026-102554](https://avd.aquasec.com/nvd/cve-2026-102554) | `com.google.guava:guava:32.0.1-jre` | `guava-32.0.1-jre.jar` | 33.7.2 |
| D05 | LOW | [CVE-2020-8908](https://avd.aquasec.com/nvd/cve-2020-8908) | `com.google.guava:guava:30.1.1-jre` | `hadoop-shaded-guava-1.1.1.jar` | 32.0.0-android |
| D05 | MEDIUM | [CVE-2023-2976](https://avd.aquasec.com/nvd/cve-2023-2976) | `com.google.guava:guava:30.1.1-jre` | `hadoop-shaded-guava-1.1.1.jar` | 32.0.0-android |
| D05 | HIGH | [CVE-2026-102554](https://avd.aquasec.com/nvd/cve-2026-102554) | `com.google.guava:guava:30.1.1-jre` | `hadoop-shaded-guava-1.1.1.jar` | 33.7.2 |
| D05 | HIGH | [CVE-2026-102554](https://avd.aquasec.com/nvd/cve-2026-102554) | `com.google.guava:guava:33.0.0-jre` | `iceberg-bundled-guava-1.5.2.jar` | 33.7.2 |
| D06 | HIGH | [CVE-2024-36114](https://avd.aquasec.com/nvd/cve-2024-36114) | `io.airlift:aircompressor:0.26` | `aircompressor-0.26.jar` | 0.27 |
| D06 | HIGH | [CVE-2025-67721](https://avd.aquasec.com/nvd/cve-2025-67721) | `io.airlift:aircompressor:0.26` | `aircompressor-0.26.jar` | 2.0.3 |
| D07 | LOW | [CVE-2026-106449](https://avd.aquasec.com/nvd/cve-2026-106449) | `at.yawk.lz4:lz4-java:1.10.2` | `lz4-java-1.10.2.jar` | 1.11.4 |
| D07 | MEDIUM | [CVE-2026-106450](https://avd.aquasec.com/nvd/cve-2026-106450) | `at.yawk.lz4:lz4-java:1.10.2` | `lz4-java-1.10.2.jar` | 1.11.4 |
| D07 | HIGH | [CVE-2026-106451](https://avd.aquasec.com/nvd/cve-2026-106451) | `at.yawk.lz4:lz4-java:1.10.2` | `lz4-java-1.10.2.jar` | 1.11.4 |
| D07 | MEDIUM | [CVE-2026-106452](https://avd.aquasec.com/nvd/cve-2026-106452) | `at.yawk.lz4:lz4-java:1.10.2` | `lz4-java-1.10.2.jar` | 1.11.2 |
| D07 | MEDIUM | [CVE-2026-106453](https://avd.aquasec.com/nvd/cve-2026-106453) | `at.yawk.lz4:lz4-java:1.10.2` | `lz4-java-1.10.2.jar` | 1.11.2 |
| D07 | MEDIUM | [CVE-2026-59949](https://avd.aquasec.com/nvd/cve-2026-59949) | `at.yawk.lz4:lz4-java:1.10.2` | `lz4-java-1.10.2.jar` | 1.11.1 |
| D08 | HIGH | [CVE-2026-108106](https://avd.aquasec.com/nvd/cve-2026-108106) | `org.xerial.snappy:snappy-java:1.1.10.5` | `snappy-java-1.1.10.5.jar` | 1.1.10.9 |
| D08 | MEDIUM | [CVE-2026-116590](https://avd.aquasec.com/nvd/cve-2026-116590) | `org.xerial.snappy:snappy-java:1.1.10.5` | `snappy-java-1.1.10.5.jar` | 1.1.10.9 |
| D08 | HIGH | [CVE-2026-93451](https://avd.aquasec.com/nvd/cve-2026-93451) | `org.xerial.snappy:snappy-java:1.1.10.5` | `snappy-java-1.1.10.5.jar` | 1.1.10.9 |
| D09 | HIGH | [CVE-2026-45799](https://avd.aquasec.com/nvd/cve-2026-45799) | `com.squareup.wire:wire-runtime-jvm:5.2.0` | `wire-runtime-jvm-5.2.0.jar` | 6.3.0, 7.0.0-alpha03 |
| D10 | HIGH | [CVE-2025-48734](https://avd.aquasec.com/nvd/cve-2025-48734) | `commons-beanutils:commons-beanutils:1.9.4` | `commons-beanutils-1.9.4.jar` | 1.11.0 |
| D11 | HIGH | [CVE-2024-25638](https://avd.aquasec.com/nvd/cve-2024-25638) | `dnsjava:dnsjava:2.1.7` | `dnsjava-2.1.7.jar` | 3.6.0 |
| D12 | MEDIUM | [CVE-2026-64607](https://avd.aquasec.com/nvd/cve-2026-64607) | `org.apache.httpcomponents.client5:httpclient5:5.3.1` | `httpclient5-5.3.1.jar` | 5.6.3 |
| D12 | HIGH | [CVE-2026-54399](https://avd.aquasec.com/nvd/cve-2026-54399) | `org.apache.httpcomponents.core5:httpcore5:5.2.4` | `httpcore5-5.2.4.jar` | 5.4.3, 5.5-beta2 |
| D12 | HIGH | [CVE-2026-54428](https://avd.aquasec.com/nvd/cve-2026-54428) | `org.apache.httpcomponents.core5:httpcore5-h2:5.2.4` | `httpcore5-h2-5.2.4.jar` | 5.4.3, 5.5-beta2 |
| D13 | MEDIUM | [CVE-2025-48924](https://avd.aquasec.com/nvd/cve-2025-48924) | `org.apache.commons:commons-lang3:3.14.0` | `commons-lang3-3.14.0.jar` | 3.18.0 |
| D14 | LOW | [CVE-2024-23454](https://avd.aquasec.com/nvd/cve-2024-23454) | `org.apache.hadoop:hadoop-common:3.3.6` | `hadoop-common-3.3.6.jar` | 3.4.0 |
