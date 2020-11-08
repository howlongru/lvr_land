# 不動產時價登錄資料處理

## Prerequisite
+ Python3
+ Docker

## Quick Run
```
bash run.sh
```

## Result
You should find result files in folder "result".

Format:
```json
{
    city: ${縣市名稱}，如: 台北市,
    time_slots: [
        {
            date: ${交易年月日} <date format: yyyy-MM-dd>,
            events: [
                {
                    district: ${鄉鎮市區}，如: 文山區,
                    building_state: ${建物型態}
                },
                ...
            ]
        }, ...
    ]
}
```