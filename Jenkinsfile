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
          env.RUN_ID = sh(returnStdout: true, script: "echo -n \"${BUILD_ID}-${JOB_NAME}\" | shasum -a 1 | cut -c1-7").trim()
          echo "run_id: ${RUN_ID}"
        }
      }
    }

    stage('log_run_id') {
      steps {
        echo "Logging run id: ${RUN_ID}"
      }
    }

    stage('em-gb-load-and-clean-load') {
      steps {
        script {
          writeFile file: 'env.load', text: '''run_id=${RUN_ID}
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
source_path=$.articles
plugin=contentstack_public_plugin.py
source={"bucket": "skyuk-uk-lan-kosmo-content-stack-ENV", "extension": "json", "directory": "public", "use_run_id": false}
'''
          sh "scripts/trigger_job.sh ${JOB_NAME} ${PROJECT_ID} ${REGION} env.load"
        }
      }
    }

    stage('em-gb-load-and-clean-metadata') {
      steps {
        script {
          writeFile file: 'env.metadata', text: '''run_id=${RUN_ID}
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
source={"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "curated"}
'''
          sh "scripts/trigger_job.sh ${JOB_NAME} ${PROJECT_ID} ${REGION} env.metadata"
        }
      }
    }

    stage('em-gb-load-and-clean-clean') {
      steps {
        script {
          writeFile file: 'env.clean', text: '''run_id=${RUN_ID}
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
rules=...  # large rules payload omitted; keep in file or pass via remote storage
'''
          sh "scripts/trigger_job.sh ${JOB_NAME} ${PROJECT_ID} ${REGION} env.clean"
        }
      }
    }

    stage('wait-for-clean-output') {
      steps {
        script {
          // check_gcs_file.py returns 0 when file exists, non-zero when not
          retry(3) {
            sh "python3 scripts/check_gcs_file.py skygenai-uk-stg-em-contentstack-public-ir-dev 'cleaned/*_${RUN_ID}.avro'"
          }
        }
      }
    }

    stage('kosmo-gb-process-kosmo-chunk') {
      steps {
        script {
          writeFile file: 'env.kosmo_chunk', text: '''run_id=${RUN_ID}
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
source={"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "cleaned"}
'''
          sh "scripts/trigger_job.sh ${JOB_NAME} ${PROJECT_ID} ${REGION} env.kosmo_chunk"
        }
      }
    }

    stage('kosmo-wait-and-embed') {
      steps {
        script {
          sh "python3 scripts/check_gcs_file.py skygenai-uk-stg-kosmo-contentstack-public-ir-dev 'chunked/*_${RUN_ID}.avro'"
          writeFile file: 'env.kosmo_embed', text: '''run_id=${RUN_ID}
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
source={"bucket": "skygenai-uk-stg-kosmo-contentstack-public-ir-ENV", "directory": "chunked"}
'''
          sh "scripts/trigger_job.sh ${JOB_NAME} ${PROJECT_ID} ${REGION} env.kosmo_embed"
        }
      }
    }

    stage('semantic-pipeline') {
      steps {
        script {
          // Example for semantic chunk -> cluster -> merge -> validate -> apply validations -> embed
          def semanticStages = [
            ['em-semantic-chunk', 'app.semantic.chunk', 'source', '{"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "cleaned"}'],
            ['em-semantic-cluster', 'app.semantic.cluster', 'source', '{"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "semantic/chunked"}'],
            ['em-semantic-merge', 'app.semantic.merge', 'source', '{"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "semantic/clustered"}'],
            ['em-semantic-validate', 'app.semantic.validate', 'source', '{"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "semantic/merged"}'],
          ]
          for (s in semanticStages) {
            writeFile file: "env_${s[0]}", text: "run_id=${RUN_ID}\nname=contentstack public\ndescription=Public articles from One Help UK\nclassification=PUBLIC\nlanguage=en\nload_type=DELTA\nplatform=em\nschedule=30 1 * * *\nmetadata__data_owner=DL-GenAI@sky.uk\nmetadata__data_owner_name=Gen AI Delivery\nmetadata__data_territory=GB\nmetadata__includes_protected=false\nmetadata__includes_critical=false\nmetadata__owners=[{\"name\": \"Gen AI Delivery\", \"email\": \"DL-GenAI@sky.uk\"}]\nmodel=gpt-4o\n${s[2]}=${s[3]}\nsemantic={\"cluster_config\": {\"eps\": 0.1, \"min_samples\": 2}}\n"
            sh "scripts/trigger_job.sh ${JOB_NAME} ${PROJECT_ID} ${REGION} env_${s[0]}"
          }
        }
      }
    }

    stage('final-embed') {
      steps {
        script {
          writeFile file: 'env.final_embed', text: '''run_id=${RUN_ID}
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
source={"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "hallucination_checked_2"}
'''
          sh "scripts/trigger_job.sh ${JOB_NAME} ${PROJECT_ID} ${REGION} env.final_embed"
        }
      }
    }

  }
  post {
    always {
      archiveArtifacts artifacts: 'env.*', allowEmptyArchive: true
    }
  }
}
