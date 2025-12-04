const express = require('express');
const { exec } = require('child_process');
const fs = require('fs');
const app = express();

app.use(express.json());

app.post('/execute', (req, res) => {
    const userCode = req.body.code || '';
    
    // 코드를 임시 파일로 저장
    fs.writeFileSync('temp.js', userCode);

    // node로 파일 실행
    exec('node temp.js', (error, stdout, stderr) => {
        if (error) {
            res.json({ status: "ERROR", output: stderr });
        } else {
            res.json({ status: "SUCCESS", output: stdout });
        }
    });
});

app.listen(8080, () => console.log('Node Agent running on 8080'));
