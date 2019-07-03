  
pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                echo 'Building..'
		sh 'python3 currency.py'
            }
        }

    }
}
