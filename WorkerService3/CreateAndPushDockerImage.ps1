#Docker should be started
Write-Output "Login to azure container registry..."
az acr login -n eoperator

Write-Output "Building image..."
docker build -f Dockerfile .. -t eoperator.azurecr.io/event-listener

Write-Output "Pushing image..."
docker push eoperator.azurecr.io/event-listener

Write-Output "Success..."
pause