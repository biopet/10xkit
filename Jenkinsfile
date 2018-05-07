pipeline {
    agent {
        node {
            label 'local'
        }
    }
    triggers {
        cron('* * * * *')
    }
    tools {
        jdk 'JDK 8u162'
        org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation 'sbt 1.0.4'
    }
    stages {
        stage('Init') {
            steps {
//            env.JAVA_HOME = "${tool 'JDK 8u162'}"
//            env.PATH = "${env.JAVA_HOME}/bin:${env.PATH}"
                sh 'java -version'
//            tool 'sbt 1.0.4'
                checkout scm
                sh 'git submodule update --init --recursive'
            }
        }

        stage('Build & Test') {
            steps {
                sh "#!/bin/bash\n" +
                        "set -e -v -o pipefail\n" +
                        "sbt -no-colors clean evicted biopetTest 'set test in assembly := {}' assembly | tee sbt.log"
                //sh "java -jar target/scala-2.11/*-assembly-*.jar -h" // Not possible for spark tools
                sh 'n=`grep -ce "\\* com.github.biopet" sbt.log || true`; if [ "$n" -ne \"0\" ]; then echo "ERROR: Found conflicting dependencies inside biopet"; exit 1; fi'
                sh "git diff --exit-code || (echo \"ERROR: Git changes detected, please regenerate the readme, create license headers and run scalafmt: sbt biopetGenerateReadme headerCreate scalafmt\" && exit 1)"
            }
        }

        stage('Results') {
            steps {
                step([$class: 'ScoveragePublisher', reportDir: 'target/scala-2.11/scoverage-report/', reportFile: 'scoverage.xml'])
                junit '**/test-output/junitreports/*.xml'
            }
        }

        stage('Example Deploy') {
            when {
                branch 'develop'
            }
            steps {
                sh "sbt -no-colors publish ghpagesPushSite"
            }
        }
    }
    post {
        always {
            junit '**/test-output/junitreports/*.xml'
        }
        failure {
            slackSend(color: '#FF0000', message: "${currentBuild.result}: Job '${env.JOB_NAME} #${env.BUILD_NUMBER}' (<${env.BUILD_URL}|Open>)", channel: '#biopet-bot', teamDomain: 'lumc', tokenCredentialId: 'lumc')
        }
        success {
            slackSend(color: '#00FF00', message: "${currentBuild.result}: Job '${env.JOB_NAME} #${env.BUILD_NUMBER}' (<${env.BUILD_URL}|Open>)", channel: '#biopet-bot', teamDomain: 'lumc', tokenCredentialId: 'lumc')
        }
    }
}