from flask import Flask, request, jsonify
import sys
from io import StringIO
import contextlib
import traceback

app = Flask(__name__)

@app.route('/execute', methods=['POST'])
def execute():
    # 사용자가 보낸 코드 (예: print("hello"))
    code = request.json.get('code', '')
    
    # print() 결과를 낚아채기 위한 버퍼
    output_buffer = StringIO()
    
    try:
        # 표준 출력을 버퍼로 돌리고 코드 실행
        with contextlib.redirect_stdout(output_buffer):
            exec(code)
        result = output_buffer.getvalue()
        return jsonify({"status": "SUCCESS", "output": result})
    except Exception:
        # 에러 나면 에러 메시지 반환
        return jsonify({"status": "ERROR", "output": traceback.format_exc()})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
