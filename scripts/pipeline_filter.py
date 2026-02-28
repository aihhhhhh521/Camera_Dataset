from __future__ import annotations
import math
import numpy as np
import cv2
from PIL import Image
from io import BytesIO
import exifread
import hashlib
from typing import Dict, Tuple, Optional

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def read_image_rgb(image_bytes: bytes) -> np.ndarray:
    img = Image.open(BytesIO(image_bytes)).convert("RGB")
    return np.array(img)  # HWC, uint8

def exif_extract(image_bytes: bytes) -> Dict[str, Optional[str]]:
    """
    尽量从 EXIF 抽取你需要的字段（可能很多站点会清 EXIF，这里允许为空）。
    """
    out = {"focal_length": None, "aperture": None, "exposure_time": None, "iso": None}
    try:
        tags = exifread.process_file(BytesIO(image_bytes), details=False)
        if "EXIF FocalLength" in tags:
            out["focal_length"] = str(tags["EXIF FocalLength"])
        if "EXIF FNumber" in tags:
            out["aperture"] = str(tags["EXIF FNumber"])
        if "EXIF ExposureTime" in tags:
            out["exposure_time"] = str(tags["EXIF ExposureTime"])
        if "EXIF ISOSpeedRatings" in tags:
            out["iso"] = str(tags["EXIF ISOSpeedRatings"])
    except Exception:
        pass
    return out

def laplacian_var(gray: np.ndarray) -> float:
    return float(cv2.Laplacian(gray, cv2.CV_64F).var())

def spectral_residual_saliency(gray: np.ndarray) -> np.ndarray:
    """
    纯 OpenCV/Numpy 实现的 Spectral Residual Saliency（不依赖 opencv-contrib）。
    返回 [0,1] 的 saliency map。
    """
    g = gray.astype(np.float32) / 255.0
    fft = np.fft.fft2(g)
    log_amp = np.log(np.abs(fft) + 1e-8)
    phase = np.angle(fft)

    # 平滑 log amplitude
    avg = cv2.blur(log_amp, (3, 3))
    residual = log_amp - avg

    sal = np.abs(np.fft.ifft2(np.exp(residual + 1j * phase))) ** 2
    sal = cv2.GaussianBlur(sal, (9, 9), 0)
    sal = (sal - sal.min()) / (sal.max() - sal.min() + 1e-8)
    return sal.astype(np.float32)

def saliency_ratio(gray: np.ndarray) -> float:
    sal = spectral_residual_saliency(gray)
    # 自适应阈值：取 top 20% 作为“高显著区域”
    thr = np.quantile(sal, 0.80)
    mask = (sal >= thr).astype(np.uint8)
    return float(mask.mean())

class DepthEstimator:
    """
    可选深度模块：默认不启用。
    启用需要：pip install torch torchvision transformers
    第一次运行会从 HF 下载模型（你要联网）。
    """
    def __init__(self, model_name: str):
        from transformers import pipeline
        self.pipe = pipeline("depth-estimation", model=model_name)

    def depth_grad_var(self, rgb: np.ndarray) -> float:
        # transformers depth-estimation 输出 PIL Image
        out = self.pipe(Image.fromarray(rgb))
        depth = np.array(out["depth"]).astype(np.float32)
        depth = (depth - depth.min()) / (depth.max() - depth.min() + 1e-8)

        gx = cv2.Sobel(depth, cv2.CV_32F, 1, 0, ksize=3)
        gy = cv2.Sobel(depth, cv2.CV_32F, 0, 1, ksize=3)
        g = gx * gx + gy * gy
        return float(g.var())

class PipelineFilter:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.depth_estimator: Optional[DepthEstimator] = None
        if cfg.get("depth_enabled", False):
            try:
                self.depth_estimator = DepthEstimator(cfg["depth_model"])
            except Exception as e:
                print(f"[WARN] depth_enabled=true but failed to init depth model: {e}")
                self.depth_estimator = None

    def validate(self, image_bytes: bytes) -> Tuple[bool, Dict[str, float], str]:
        """
        返回：是否通过、metrics、reject_reason（通过则 reason=""）
        """
        metrics: Dict[str, float] = {}

        # 基础：文件大小
        kb = len(image_bytes) / 1024.0
        metrics["filesize_kb"] = kb
        if kb < float(self.cfg.get("min_filesize_kb", 1)):
            return False, metrics, "filesize_too_small"

        # 解码
        rgb = read_image_rgb(image_bytes)
        h, w = rgb.shape[:2]
        metrics["H"] = float(h)
        metrics["W"] = float(w)

        # 基础：分辨率
        min_side = min(h, w)
        metrics["min_side"] = float(min_side)
        if min_side < int(self.cfg.get("min_side", 0)):
            return False, metrics, "min_side_too_small"

        # 基础：长宽比极端过滤（避免 10:1 全景）
        ar = max(h, w) / max(1, min(h, w))
        metrics["aspect_ratio"] = float(ar)
        if ar > float(self.cfg.get("max_aspect_ratio", 999)):
            return False, metrics, "aspect_ratio_too_extreme"

        gray = cv2.cvtColor(rgb, cv2.COLOR_RGB2GRAY)

        # 清晰度：Laplacian 方差
        lv = laplacian_var(gray)
        metrics["laplacian_var"] = lv
        if lv < float(self.cfg.get("min_laplacian_var", 0)):
            return False, metrics, "too_blurry"

        # 构图显著性：显著区域占比
        if self.cfg.get("saliency_enabled", True):
            sr = saliency_ratio(gray)
            metrics["subject_saliency_ratio"] = sr
            if sr < float(self.cfg.get("saliency_min_ratio", 0.0)):
                return False, metrics, "saliency_too_low"
            if sr > float(self.cfg.get("saliency_max_ratio", 1.0)):
                return False, metrics, "saliency_too_high"

        # 可选：深度一致性（深度梯度方差）
        if self.cfg.get("depth_enabled", False) and self.depth_estimator is not None:
            dvar = self.depth_estimator.depth_grad_var(rgb)
            metrics["depth_grad_var"] = dvar
            if dvar < float(self.cfg.get("depth_min_grad_var", 0.0)):
                return False, metrics, "depth_grad_var_too_low"
            # 你也可以加“上限”过滤碎片化：例如 if dvar > X: reject

        return True, metrics, ""