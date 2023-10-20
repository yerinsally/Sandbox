## 첫 실행 시 명령어
1. Node.js 다운로드
- 명령 프롬프트 열고 다음 코드 실행
    ```
    node -v
    npm -v
    npm install -g playwright
    install playwright
    playwright install
    playwright codegen instagram.com
    ```
2. 만약 오류난다면, Powershell 관리자 권한으로 실행 후 코드 차례대로 입력
    ```
    Set-ExecutionPolicy RemoteSigned
    Get-ExecutionPolicy # RemoteSigned
    playwright install
    
    # 터미널 창에서 다시 코드 입력
    pip install playwright
    playwright install
    playwright codegen instagram.com
    ```
