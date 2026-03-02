# -*- coding: utf-8 -*-
"""
飞书消息推送模块
"""
import requests
import json
import hmac
import hashlib
import base64
import time
from typing import List, Dict, Optional
import logging
import os

logger = logging.getLogger(__name__)


class FeishuReporter:
    """飞书消息推送器"""
    
    def __init__(self, webhook_url: str = None, secret: str = None):
        """
        初始化飞书推送器
        
        Args:
            webhook_url: 飞书机器人Webhook地址
            secret: 飞书机器人密钥
        """
        self.webhook_url = webhook_url or os.getenv("FEISHU_WEBHOOK", "")
        self.secret = secret or os.getenv("FEISHU_SECRET", "")
    
    def _generate_sign(self) -> str:
        """
        生成飞书签名
        """
        if not self.secret:
            return ""
        
        # 当前时间戳
        timestamp = str(int(time.time()))
        
        # 拼接字符串
        string_to_sign = f"{timestamp}\n{self.secret}"
        
        # HMAC-SHA256 签名
        hmac_code = hmac.new(
            string_to_sign.encode("utf-8"),
            digestmod=hashlib.sha256
        ).digest()
        
        # Base64 编码
        sign = base64.b64encode(hmac_code).decode("utf-8")
        
        return sign, timestamp
    
    def send_text(self, text: str) -> bool:
        """
        发送文本消息
        """
        if not self.webhook_url:
            logger.warning("未配置飞书Webhook URL")
            return False
        
        sign, timestamp = self._generate_sign()
        
        payload = {
            "msg_type": "text",
            "content": {
                "text": text
            }
        }
        
        if sign:
            payload["timestamp"] = timestamp
            payload["sign"] = sign
        
        try:
            response = requests.post(
                self.webhook_url,
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get("code") == 0:
                    logger.info("飞书消息发送成功")
                    return True
                else:
                    logger.error(f"飞书API错误: {result}")
            else:
                logger.error(f"HTTP请求失败: {response.status_code}")
            
            return False
        except Exception as e:
            logger.error(f"发送飞书消息失败: {e}")
            return False
    
    def send_rich_text(
        self,
        title: str,
        content: List[List[Dict]],
    ) -> bool:
        """
        发送富文本消息
        
        Args:
            title: 标题
            content: 内容 (二维数组，每行是一个段落)
        """
        if not self.webhook_url:
            logger.warning("未配置飞书Webhook URL")
            return False
        
        sign, timestamp = self._generate_sign()
        
        # 构建富文本内容
        elements = []
        
        # 标题
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**{title}**",
                "style": {"bold": True, "font_size": "large"}
            }
        })
        
        # 分割线
        elements.append({"tag": "hr"})
        
        # 内容
        for row in content:
            text_content = ""
            for item in row:
                if item.get("type") == "text":
                    text_content += item.get("content", "")
                elif item.get("type") == "link":
                    text_content += f"[{item.get('content', '')}]({item.get('url', '')})"
            
            if text_content:
                elements.append({
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": text_content
                    }
                })
        
        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {
                        "tag": "plain_text",
                        "content": title,
                        "style": {"bold": True}
                    },
                    "template": "blue"
                },
                "elements": elements
            }
        }
        
        if sign:
            payload["timestamp"] = timestamp
            payload["sign"] = sign
        
        try:
            response = requests.post(
                self.webhook_url,
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get("code") == 0:
                    logger.info("飞书富文本消息发送成功")
                    return True
                else:
                    logger.error(f"飞书API错误: {result}")
            
            return False
        except Exception as e:
            logger.error(f"发送飞书富文本消息失败: {e}")
            return False
    
    def send_signal_report(self, signals: List) -> bool:
        """
        发送ETF信号报告
        """
        from signals.generator import SignalType
        
        if not signals:
            return self.send_text("📊 暂无ETF信号数据")
        
        # 构建消息
        lines = []
        lines.append("📊 **ETF量化分析信号报告**")
        lines.append("")
        
        # 统计
        strong_buy = len([s for s in signals if s.signal == SignalType.STRONG_BUY])
        buy = len([s for s in signals if s.signal == SignalType.BUY])
        hold = len([s for s in signals if s.signal == SignalType.HOLD])
        sell = len([s for s in signals if s.signal == SignalType.SELL])
        strong_sell = len([s for s in signals if s.signal == SignalType.STRONG_SELL])
        
        lines.append(f"📈 信号分布: 强烈买入 {strong_buy} | 买入 {buy} | 持有 {hold} | 卖出 {sell} | 强烈卖出 {strong_sell}")
        lines.append("")
        
        # 强烈买入
        if strong_buy > 0:
            lines.append("🟢 **强烈买入**:")
            for s in sorted(signals, key=lambda x: x.score, reverse=True)[:5]:
                if s.signal == SignalType.STRONG_BUY:
                    price_str = f"{s.price:.3f}" if s.price else "N/A"
                    change_str = f"{s.change_pct:+.2f}%" if s.change_pct else "N/A"
                    lines.append(f"• {s.symbol} {s.name} | {price_str} ({change_str}) | 评分: {s.score:.2f}")
            lines.append("")
        
        # 买入
        if buy > 0:
            lines.append("🟢 **买入推荐**:")
            for s in sorted(signals, key=lambda x: x.score, reverse=True)[strong_buy:strong_buy+5]:
                if s.signal == SignalType.BUY:
                    price_str = f"{s.price:.3f}" if s.price else "N/A"
                    change_str = f"{s.change_pct:+.2f}%" if s.change_pct else "N/A"
                    lines.append(f"• {s.symbol} {s.name} | {price_str} ({change_str}) | 评分: {s.score:.2f}")
            lines.append("")
        
        # 卖出
        if sell > 0:
            lines.append("🔴 **卖出建议**:")
            for s in sorted(signals, key=lambda x: x.score)[:5]:
                if s.signal == SignalType.SELL:
                    price_str = f"{s.price:.3f}" if s.price else "N/A"
                    change_str = f"{s.change_pct:+.2f}%" if s.change_pct else "N/A"
                    lines.append(f"• {s.symbol} {s.name} | {price_str} ({change_str}) | 评分: {s.score:.2f}")
            lines.append("")
        
        lines.append("")
        lines.append("💡 提示: 本报告仅供参考，不构成投资建议")
        
        return self.send_text("\n".join(lines))
    
    def send_daily_report(
        self,
        strong_buy: List = None,
        buy: List = None,
        hold: List = None,
        sell: List = None,
        market_summary: str = ""
    ) -> bool:
        """
        发送每日分析报告
        """
        lines = []
        lines.append("📊 **ETF量化每日分析报告**")
        lines.append("")
        lines.append(f"⏰ 更新时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")
        
        if market_summary:
            lines.append(f"📈 **市场概况**:")
            lines.append(market_summary)
            lines.append("")
        
        # 强烈买入
        if strong_buy:
            lines.append("🟢 **强烈买入** (建议重点关注):")
            for s in strong_buy[:5]:
                lines.append(f"• {s.symbol} {s.name} | {s.price:.3f} ({s.change_pct:+.2f}%)")
            lines.append("")
        
        # 买入
        if buy:
            lines.append("🟡 **买入** (可适当关注):")
            for s in buy[:5]:
                lines.append(f"• {s.symbol} {s.name} | {s.price:.3f} ({s.change_pct:+.2f}%)")
            lines.append("")
        
        # 持有
        if hold:
            lines.append("⚪ **持有** (继续观察):")
            for s in hold[:5]:
                lines.append(f"• {s.symbol} {s.name} | {s.price:.3f} ({s.change_pct:+.2f}%)")
            lines.append("")
        
        # 卖出
        if sell:
            lines.append("🔴 **卖出** (建议规避):")
            for s in sell[:5]:
                lines.append(f"• {s.symbol} {s.name} | {s.price:.3f} ({s.change_pct:+.2f}%)")
            lines.append("")
        
        lines.append("")
        lines.append("---")
        lines.append("💡 提示: 本报告仅供参考，投资需谨慎")
        
        return self.send_text("\n".join(lines))


def test_webhook(webhook_url: str, secret: str = None) -> bool:
    """
    测试Webhook是否可用
    """
    reporter = FeishuReporter(webhook_url, secret)
    return reporter.send_text("🧡 虾宝量化系统测试消息 - 连接成功！")


if __name__ == "__main__":
    # 测试
    test_webhook(os.getenv("FEISHU_WEBHOOK", ""))
