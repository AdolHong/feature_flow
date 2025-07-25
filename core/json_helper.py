import json
import pandas as pd
import numpy as np

class UniversalEncoder(json.JSONEncoder):
    """
    一个可以处理 Pandas DataFrame, Timestamp, NaN, 以及 NumPy 类型的通用编码器。
    """
    def default(self, obj):
        # 处理 Pandas DataFrame
        if isinstance(obj, pd.DataFrame):
            # 将 DataFrame 转换为字典，并添加类型元数据
            return {
                "__type__": "DataFrame",
                "data": obj.to_dict(orient="tight")
            }
        
        # 处理 Pandas Timestamp
        if isinstance(obj, pd.Timestamp):
            return {
                "__type__": "Timestamp",
                "value": obj.isoformat()
            }
        
        # 处理 NaN 和 NaT
        if pd.isna(obj):
            return None
        
        # 处理 NumPy 类型，因为它们不被 json 库识别
        if isinstance(obj, (np.integer, np.floating, np.bool_)):
            return obj.item()

        # 对于其他类型，使用默认编码器
        return super().default(obj)

def universal_decoder(dct):
    """
    根据元数据将字典还原为 Pandas DataFrame 或其他对象。
    """
    # 检查字典中是否有 __type__ 键
    if "__type__" in dct:
        data_type = dct["__type__"]
        
        # 还原 Pandas DataFrame
        if data_type == "DataFrame":
            return pd.DataFrame.from_dict(dct["data"], orient="tight")
        
        # 还原 Pandas Timestamp
        if data_type == "Timestamp":
            return pd.Timestamp(dct["value"])
            
    return dct