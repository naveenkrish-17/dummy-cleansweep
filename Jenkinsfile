pipeline {
  agent any
  environment {
    PROJECT_ID = credentials('GOOGLE_CLOUD_PROJECT')
    REGION = 'europe-west1'
    JOB_NAME = 'cleansweep-dev'
  }
  parameters {
    string(name: 'GCP_PROJECT', defaultValue: '', description: 'Optional override for GCP project')
  }
  stages {
    stage('Prepare') {
      steps {
        script {
          if (params.GCP_PROJECT?.trim()) {
            env.PROJECT_ID = params.GCP_PROJECT.trim()
          }
          env.RUN_ID = sh(returnStdout: true, script: "echo -n \"${BUILD_ID}-contentstack-public\" | shasum -a 1 | cut -c1-7").trim()
          echo "run_id: ${RUN_ID}"
        }
      }
    }

    stage('log_run_id') {
      steps {
        echo "Logging run_id: ${RUN_ID}"
      }
    }

    stage('em-gb-load-and-clean-load') {
      steps {
        script {
          def envContent = """run_id=${RUN_ID}
name=contentstack public
description=Public articles from One Help UK
classification=PUBLIC
language=en
load_type=DELTA
platform=em
schedule=30 1 * * *
metadata__data_owner=DL-GenAI@sky.uk
metadata__data_owner_name=Gen AI Delivery
metadata__data_territory=GB
metadata__includes_protected=false
metadata__includes_critical=false
metadata__owners=[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]
mapping=content_stack_mapping.json
source_path=\$.articles
plugin=contentstack_public_plugin.py
source={"bucket": "skyuk-uk-lan-kosmo-content-stack-ENV", "extension": "json", "directory": "public", "use_run_id": false}"""
          writeFile file: 'env.load', text: envContent
          sh "scripts/trigger_job.sh '${JOB_NAME}' '${PROJECT_ID}' '${REGION}' 'app.transform' env.load"
        }
      }
    }

    stage('em-gb-load-and-clean-metadata') {
      steps {
        script {
          def envContent = """run_id=${RUN_ID}
name=contentstack public
description=Public articles from One Help UK
classification=PUBLIC
language=en
load_type=DELTA
platform=em
schedule=30 1 * * *
metadata__data_owner=DL-GenAI@sky.uk
metadata__data_owner_name=Gen AI Delivery
metadata__data_territory=GB
metadata__includes_protected=false
metadata__includes_critical=false
metadata__owners=[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]
plugin=contentstack_public_plugin.py
source={"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "curated"}"""
          writeFile file: 'env.metadata', text: envContent
          sh "scripts/trigger_job.sh '${JOB_NAME}' '${PROJECT_ID}' '${REGION}' 'app.metadata' env.metadata"
        }
      }
    }

    stage('em-gb-load-and-clean-clean') {
      steps {
        script {
          def envContent = """run_id=${RUN_ID}
name=contentstack public
description=Public articles from One Help UK
classification=PUBLIC
language=en
load_type=DELTA
platform=em
schedule=30 1 * * *
metadata__data_owner=DL-GenAI@sky.uk
metadata__data_owner_name=Gen AI Delivery
metadata__data_territory=GB
metadata__includes_protected=false
metadata__includes_critical=false
metadata__owners=[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]
source={"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "metadata"}
rules=[{"rule": "filter out non-searchable", "type": "filter_by_column", "column": "id", "operator": "not in", "value": ["blt4907f09ceba3135a", "blta5a34d8b4f0b4d01", "blt475bf968ff6c8682", "bltc447a246664ebe5c", "blt7f5509e7b3f6924b", "blt622ba69fc5c93f1f"]}, {"rule": "filter out blank Articles", "type": "filter_by_column", "column": "length", "operator": ">", "value": 0}, {"rule": "Remove ROI articles", "type": "filter_by_column", "column": "metadata_region", "operator": "in", "value": "GB"}, {"rule": "Remove -roi- articles slug contains", "type": "filter_by_column", "column": "metadata_slug", "operator": "not in", "value": "-roi-"}, {"rule": "Remove roi articles slug ending", "type": "remove_by_match", "column": "metadata_slug", "value": "^.*-roi\$"}, {"rule": "Remove versioned and test articles slug contains", "type": "remove_by_match", "column": "metadata_slug", "value": "-(?:v|t)[0-9]+"}, {"rule": "Remove ab articles slug endings", "type": "remove_by_match", "column": "metadata_slug", "value": "^.*-(?:b|m1)\$"}, {"rule": "Remove old articles slug ending", "type": "remove_by_match", "column": "metadata_slug", "value": "^.*-old\$"}, {"rule": "Remove duplicate articles", "type": "remove_duplicates", "columns": ["metadata_slug"], "order_by": "metadata_modified", "order": "desc"}, {"rule": "Remove articles with redirect", "type": "filter_by_column", "column": "metadata_should_redirect", "operator": "=", "value": false}, {"rule": "Remove articles containing test", "type": "remove_by_match", "column": "title", "value": "test"}, {"rule": "Remove substrings from title", "type": "remove_substrings", "columns": ["title"], "substrings": ["((?:\\\\\\\\[?(?:redirect(?:ed)?|(?:un)?searchable)\\\\\\\\]?)|v\\\\\\\\d|(?:ab | - |\\\\\\\\[)?test\\\\\\\\]?|\\\\\\\\[\\\\\\\\])"]}, {"rule": "Replace carriage returns", "type": "replace_substrings", "columns": ["content"], "substrings": ["\\\\r"], "replacement": "\\\\n"}, {"rule": "Remove substrings from content", "type": "remove_substrings", "columns": ["content"], "substrings": ["&nbsp;", "-{2,}"]}, {"rule": "Replace additional spaces", "type": "replace_substrings", "columns": ["content"], "substrings": [" {2,}"], "replacement": " "}, {"rule": "Replace superfluous returns", "type": "replace_substrings", "columns": ["content"], "substrings": ["\\\\n{3,}"], "replacement": "\\\\n\\\\n"}, {"rule": "Remove ID&V articles", "type": "remove_by_match", "column": "title", "value": "ID&V"}, {"rule": "Remove Atlas articles", "type": "remove_by_match", "column": "title", "value": "Atlas"}, {"rule": "Remove PRIVATE articles", "type": "remove_by_match", "column": "content_type", "value": "PRIVATE"}, {"rule": "refactor markdown links", "type": "reference_to_inline", "column": "content"}]"""
          writeFile file: 'env.clean', text: envContent
          sh "scripts/trigger_job.sh '${JOB_NAME}' '${PROJECT_ID}' '${REGION}' 'app.clean' env.clean"
        }
      }
    }

    stage('em-gb-load-and-clean-check-new-file-produced') {
      steps {
        script {
          retry(5) {
            sleep(time: 10, unit: 'SECONDS')
            sh "python3 scripts/check_gcs_file.py skygenai-uk-stg-em-contentstack-public-ir-dev 'cleaned/*_${RUN_ID}.avro'"
          }
        }
      }
    }

    stage('Parallel: Kosmo & EM Processing') {
      parallel {
        stage('Kosmo Pipeline') {
          stages {
            stage('kosmo-gb-process-kosmo-chunk') {
              steps {
                script {
                  def envContent = """run_id=${RUN_ID}
name=contentstack public
description=Public articles from One Help UK
classification=PUBLIC
language=en
load_type=DELTA
platform=kosmo
schedule=30 1 * * *
metadata__data_owner=DL-GenAI@sky.uk
metadata__data_owner_name=Gen AI Delivery
metadata__data_territory=GB
metadata__includes_protected=false
metadata__includes_critical=false
metadata__owners=[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]
source={"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "cleaned"}"""
                  writeFile file: 'env.kosmo_chunk', text: envContent
                  sh "scripts/trigger_job.sh '${JOB_NAME}' '${PROJECT_ID}' '${REGION}' 'app.chunk' env.kosmo_chunk"
                }
              }
            }

            stage('kosmo-gb-process-kosmo-check-new-file-produced') {
              steps {
                script {
                  retry(5) {
                    sleep(time: 10, unit: 'SECONDS')
                    sh "python3 scripts/check_gcs_file.py skygenai-uk-stg-kosmo-contentstack-public-ir-dev 'chunked/*_${RUN_ID}.avro'"
                  }
                }
              }
            }

            stage('kosmo-gb-process-kosmo-embed') {
              steps {
                script {
                  def envContent = """run_id=${RUN_ID}
name=contentstack public
description=Public articles from One Help UK
classification=PUBLIC
language=en
load_type=DELTA
platform=kosmo
schedule=30 1 * * *
metadata__data_owner=DL-GenAI@sky.uk
metadata__data_owner_name=Gen AI Delivery
metadata__data_territory=GB
metadata__includes_protected=false
metadata__includes_critical=false
metadata__owners=[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]
source={"bucket": "skygenai-uk-stg-kosmo-contentstack-public-ir-ENV", "directory": "chunked"}"""
                  writeFile file: 'env.kosmo_embed', text: envContent
                  sh "scripts/trigger_job.sh '${JOB_NAME}' '${PROJECT_ID}' '${REGION}' 'app.embed' env.kosmo_embed"
                }
              }
            }
          }
        }

        stage('EM Semantic Pipeline') {
          stages {
            stage('em-gb-process-em-chunk-semantic-chunk') {
              steps {
                script {
                  def envContent = """run_id=${RUN_ID}
name=contentstack public
description=Public articles from One Help UK
classification=PUBLIC
language=en
load_type=DELTA
platform=em
schedule=30 1 * * *
metadata__data_owner=DL-GenAI@sky.uk
metadata__data_owner_name=Gen AI Delivery
metadata__data_territory=GB
metadata__includes_protected=false
metadata__includes_critical=false
metadata__owners=[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]
model=gpt-4o
source={"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "cleaned"}
semantic={"cluster_config": {"eps": 0.1, "min_samples": 2}}"""
                  writeFile file: 'env.semantic_chunk', text: envContent
                  sh "scripts/trigger_job.sh '${JOB_NAME}' '${PROJECT_ID}' '${REGION}' 'app.semantic.chunk' env.semantic_chunk"
                }
              }
            }

            stage('em-gb-process-em-chunk-semantic-cluster') {
              steps {
                script {
                  def envContent = """run_id=${RUN_ID}
name=contentstack public
description=Public articles from One Help UK
classification=PUBLIC
language=en
load_type=DELTA
platform=em
schedule=30 1 * * *
metadata__data_owner=DL-GenAI@sky.uk
metadata__data_owner_name=Gen AI Delivery
metadata__data_territory=GB
metadata__includes_protected=false
metadata__includes_critical=false
metadata__owners=[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]
model=gpt-4o
source={"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "semantic/chunked"}
semantic={"cluster_config": {"eps": 0.1, "min_samples": 2}}"""
                  writeFile file: 'env.semantic_cluster', text: envContent
                  sh "scripts/trigger_job.sh '${JOB_NAME}' '${PROJECT_ID}' '${REGION}' 'app.semantic.cluster' env.semantic_cluster"
                }
              }
            }

            stage('em-gb-process-em-chunk-semantic-merge') {
              steps {
                script {
                  def envContent = """run_id=${RUN_ID}
name=contentstack public
description=Public articles from One Help UK
classification=PUBLIC
language=en
load_type=DELTA
platform=em
schedule=30 1 * * *
metadata__data_owner=DL-GenAI@sky.uk
metadata__data_owner_name=Gen AI Delivery
metadata__data_territory=GB
metadata__includes_protected=false
metadata__includes_critical=false
metadata__owners=[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]
model=gpt-4o
source={"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "semantic/clustered"}
semantic={"cluster_config": {"eps": 0.1, "min_samples": 2}}"""
                  writeFile file: 'env.semantic_merge', text: envContent
                  sh "scripts/trigger_job.sh '${JOB_NAME}' '${PROJECT_ID}' '${REGION}' 'app.semantic.merge' env.semantic_merge"
                }
              }
            }

            stage('em-gb-process-em-chunk-semantic-validate') {
              steps {
                script {
                  def envContent = """run_id=${RUN_ID}
name=contentstack public
description=Public articles from One Help UK
classification=PUBLIC
language=en
load_type=DELTA
platform=em
schedule=30 1 * * *
metadata__data_owner=DL-GenAI@sky.uk
metadata__data_owner_name=Gen AI Delivery
metadata__data_territory=GB
metadata__includes_protected=false
metadata__includes_critical=false
metadata__owners=[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]
model=gpt-4o
source={"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "semantic/merged"}
semantic={"cluster_config": {"eps": 0.1, "min_samples": 2}}"""
                  writeFile file: 'env.semantic_validate', text: envContent
                  sh "scripts/trigger_job.sh '${JOB_NAME}' '${PROJECT_ID}' '${REGION}' 'app.semantic.validate' env.semantic_validate"
                }
              }
            }

            stage('em-gb-process-em-apply-watson-validation') {
              steps {
                script {
                  def envContent = """run_id=${RUN_ID}
name=contentstack public
description=Public articles from One Help UK
classification=PUBLIC
language=en
load_type=DELTA
platform=em
schedule=30 1 * * *
metadata__data_owner=DL-GenAI@sky.uk
metadata__data_owner_name=Gen AI Delivery
metadata__data_territory=GB
metadata__includes_protected=false
metadata__includes_critical=false
metadata__owners=[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]
plugin=watson_validate.py
source={"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "chunked"}"""
                  writeFile file: 'env.watson_validation', text: envContent
                  sh "scripts/trigger_job.sh '${JOB_NAME}' '${PROJECT_ID}' '${REGION}' 'app.run' env.watson_validation"
                }
              }
            }

            stage('em-gb-process-em-apply-language-validation') {
              steps {
                script {
                  def envContent = """run_id=${RUN_ID}
name=contentstack public
description=Public articles from One Help UK
classification=PUBLIC
language=en
load_type=DELTA
platform=em
schedule=30 1 * * *
metadata__data_owner=DL-GenAI@sky.uk
metadata__data_owner_name=Gen AI Delivery
metadata__data_territory=GB
metadata__includes_protected=false
metadata__includes_critical=false
metadata__owners=[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]
plugin=language_validate.py
source={"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "watson_fixed"}"""
                  writeFile file: 'env.language_validation', text: envContent
                  sh "scripts/trigger_job.sh '${JOB_NAME}' '${PROJECT_ID}' '${REGION}' 'app.run' env.language_validation"
                }
              }
            }

            stage('em-gb-process-em-apply-URL-check-and-metadata-dedupe') {
              steps {
                script {
                  def envContent = """run_id=${RUN_ID}
name=contentstack public
description=Public articles from One Help UK
classification=PUBLIC
language=en
load_type=DELTA
platform=em
schedule=30 1 * * *
metadata__data_owner=DL-GenAI@sky.uk
metadata__data_owner_name=Gen AI Delivery
metadata__data_territory=GB
metadata__includes_protected=false
metadata__includes_critical=false
metadata__owners=[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]
plugin=contentstack_public_plugin.py
source={"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "language_fixed"}"""
                  writeFile file: 'env.url_check', text: envContent
                  sh "scripts/trigger_job.sh '${JOB_NAME}' '${PROJECT_ID}' '${REGION}' 'app.run' env.url_check"
                }
              }
            }

            stage('em-gb-process-em-apply-hallucination-check') {
              steps {
                script {
                  def envContent = """run_id=${RUN_ID}
name=contentstack public
description=Public articles from One Help UK
classification=PUBLIC
language=en
load_type=DELTA
platform=em
schedule=30 1 * * *
metadata__data_owner=DL-GenAI@sky.uk
metadata__data_owner_name=Gen AI Delivery
metadata__data_territory=GB
metadata__includes_protected=false
metadata__includes_critical=false
metadata__owners=[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]
plugin=hallucination_check.py
source={"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "url_check"}"""
                  writeFile file: 'env.hallucination_check', text: envContent
                  sh "scripts/trigger_job.sh '${JOB_NAME}' '${PROJECT_ID}' '${REGION}' 'app.run' env.hallucination_check"
                }
              }
            }

            stage('em-gb-process-em-apply-2nd-hallucination-check') {
              steps {
                script {
                  def envContent = """run_id=${RUN_ID}
name=contentstack public
description=Public articles from One Help UK
classification=PUBLIC
language=en
load_type=DELTA
platform=em
schedule=30 1 * * *
metadata__data_owner=DL-GenAI@sky.uk
metadata__data_owner_name=Gen AI Delivery
metadata__data_territory=GB
metadata__includes_protected=false
metadata__includes_critical=false
metadata__owners=[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]
plugin=hallucination_check_2.py
source={"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "hallucination_checked"}"""
                  writeFile file: 'env.hallucination_check_2', text: envContent
                  sh "scripts/trigger_job.sh '${JOB_NAME}' '${PROJECT_ID}' '${REGION}' 'app.run' env.hallucination_check_2"
                }
              }
            }

            stage('em-gb-process-em-embed') {
              steps {
                script {
                  def envContent = """run_id=${RUN_ID}
name=contentstack public
description=Public articles from One Help UK
classification=PUBLIC
language=en
load_type=DELTA
platform=em
schedule=30 1 * * *
metadata__data_owner=DL-GenAI@sky.uk
metadata__data_owner_name=Gen AI Delivery
metadata__data_territory=GB
metadata__includes_protected=false
metadata__includes_critical=false
metadata__owners=[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]
model=text-embedding-3-large
dimensions=2000
source={"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "hallucination_checked_2"}"""
                  writeFile file: 'env.em_embed', text: envContent
                  sh "scripts/trigger_job.sh '${JOB_NAME}' '${PROJECT_ID}' '${REGION}' 'app.embed' env.em_embed"
                }
              }
            }
          }
        }
      }
    }
  }
  post {
    always {
      archiveArtifacts artifacts: 'env.*', allowEmptyArchive: true
    }
    success {
      echo "Pipeline completed successfully for run_id: ${RUN_ID}"
    }
    failure {
      echo "Pipeline failed for run_id: ${RUN_ID}"
    }
  }
}
