# Umami (EdgeOne Pages 移植版)

> [!WARNING]
> **EdgeOne Pages 运行时兼容性严重警告**
>
> 经排查确认，EdgeOne Pages 平台在 **2025年12月21日** 左右更新了其 Node.js/Next.js 运行时适配器，引入了一个底层缺陷，导致在此日期之后部署的 Umami 实例无法正常工作（主要表现为 API 请求体丢失，导致登录失败等 400 错误）。
>
> **影响范围说明：**
> *   🔴 **受影响**：**2025年12月21日之后**新建，或在此日期之后执行过重新构建/部署的项目。
> *   🟢 **不受影响**：在此日期之前部署，且之后**未进行过任何更新**的旧存量项目（请务必避免触发重新构建，否则将受到影响）。
>
> **安全提示：**
> 为了协助官方定位修复该问题，当前代码库已临时植入了大量深度调试日志（Debug Logs）。这些日志可能会在构建日志或运行时输出中暴露敏感信息。**在官方修复此问题并移除本警告之前，建议普通用户暂停部署或更新。**

> [!NOTE]
> 我们已在 https://github.com/afoim/umami-edgeonepages/tree/main 短暂性修复该问题。由于EO吃POST请求，我们全部换成了GET请求 

### 漏洞详情与复现报告

# Bug Report: Next.js App Router Request Body Prematurely Consumed on EdgeOne Pages

## Summary
在 Tencent Cloud EdgeOne Pages (基于 SCF) 环境下部署 Next.js (App Router) 应用时，API Route 接收到的 `Request` 对象的 Body Stream 已经被提前消费 (Consumed/Drained)，导致应用层无法读取请求体。

### 关键现象与复现条件
*   **环境差异**：该 Bug **仅在生产环境**复现，本地开发环境（`npm run dev` / `start`）一切正常。
*   **调试困境**：目前的开发体验非常痛苦，因为无法在本地模拟生产环境的边缘行为。强烈建议官方提供一个能够完全模拟 EdgeOne Pages 生产环境（包括 SCF 适配器行为）的本地 Docker 镜像或 CLI 工具，避免开发者只能通过反复部署到云端来 Debug。
*   **受影响接口**：所有被 Next.js App Router 包装的 API 接口（如 `/api/auth/login`, `/api/send` 等）。
*   **具体表现**：只要请求方法为 **POST**，无论 Headers 如何设置或 Body 是否有值，Next.js 业务层收到的 Request Body **始终为空**。
*   **代码隔离测试**：如果在非 EdgeOne 部署的 Next.js 环境中单独提取并运行相同的 Body 读取逻辑（包括 `req.clone()`, `req.text()`, `req.json()`），均能正常工作。这进一步证实了问题出在 EdgeOne Pages 的运行时适配器层，而非用户代码逻辑。
*   **推测原因**：怀疑是 EdgeOne Pages 在处理 Next.js 高级特性（如 SSR/RSC）的请求透传逻辑中，与其他模块协作时发生了冲突，导致传递给业务层的 Request Stream 已经是“死流”（Drained Stream）。

### 关于 Rewrites/Redirects 支持的变更
文档 [EdgeOne Pages Next.js 指南](https://pages.edgeone.ai/zh/document/framework-nextjs) 中曾提到 **“目前暂不支持 Next.js 的重写和重定向”**。但实测发现，目前生产环境似乎已经支持了这些特性。
**值得注意的是**：在该项目正常运行的旧版本时期，这些特性确实是不支持的。请 EdgeOne 开发团队审查最近关于 Rewrites/Redirects 支持的代码提交，这可能是导致 Body Stream 异常的副作用来源。

## Temporary Workaround
作为临时解决方案，本项目（Umami EdgeOne 移植版）已被迫**将所有关键的 POST 接口修改为 GET 接口**以绕过此 Bug。这虽然能让服务跑通，但破坏了 RESTful 规范且存在安全隐患，急需官方修复。

## Environment
- **Platform**: Tencent Cloud EdgeOne Pages (Serverless Cloud Function / SCF)
- **Framework**: Next.js (App Router)
- **Deployment Type**: Serverless / Edge
- **Issue Scope**: All API Routes handling POST/PUT requests with body

## Reproduction Steps (POC)

### 1. POC Code
在 Next.js 项目中创建 `src/app/api/debug-poc/route.ts`：

```typescript
import { NextResponse } from 'next/server';

export async function POST(request: Request) {
  const logs: string[] = [];
  const log = (msg: string) => logs.push(msg);

  log(`[Start] Method: ${request.method}`);
  
  // 1. Check Headers
  const headers = Object.fromEntries(request.headers);
  const contentLength = headers['content-length'];
  log(`[Headers] content-length: ${contentLength}`);
  log(`[Headers] content-type: ${headers['content-type']}`);

  // 2. Check Body Status
  log(`[Status] request.bodyUsed before read: ${request.bodyUsed}`);

  // 3. Attempt Read
  try {
      const text = await request.text();
      log(`[Result] req.text() returned length: ${text.length}`);
      if (text.length === 0 && Number(contentLength) > 0) {
          log(`[FATAL] Content-Length is ${contentLength} but body text is empty!`);
      }
  } catch (e: any) {
      log(`[Error] req.text() failed: ${e.message}`);
  }

  // 4. Attempt JSON (to trigger specific stream error)
  try {
      // Re-reading specific error message
      await request.json();
  } catch (e: any) {
      log(`[Error] req.json() failed: ${e.message}`);
  }

  return NextResponse.json({
    platform: 'EdgeOne Pages',
    headers: { 'content-length': contentLength },
    logs
  });
}
```

### 2. Execution
发送一个带有 Body 的 POST 请求：
```bash
curl -X POST "https://your-site.edgeone.cool/api/debug-poc" \
     -H "Content-Type: application/json" \
     -d '{"test": "hello"}'
```

## Observed Logs
实际运行结果如下（基于真实环境调试）：

```json
{
    "headers": {
        "content-length": "46"
    },
    "logs": [
        "[Start] Method: POST",
        "[Headers] content-length: 46",
        "[Headers] content-type: application/json",
        "[Status] request.bodyUsed before read: false", 
        "[Result] req.text() returned length: 0",
        "[FATAL] Content-Length is 46 but body text is empty!",
        "[Error] req.json() failed: Body is unusable: Body has already been read"
    ]
}
```

## Technical Analysis
1.  **Stream Drained**: `req.json()` 抛出的错误 `Body has already been read` 是最直接的证据。这表明底层的 ReadableStream 已经被读取过。
2.  **Adapter Issue**: 在 Next.js App Router 中，`Request` 对象应由适配器根据入站事件（如 SCF Event）构建。如果适配器在构建 Request 对象时（例如为了处理 Base64 编码、日志记录或 WAF 检查）读取了流，但没有使用 `tee()` 分流或重置流，应用层拿到的就是枯竭的流。
3.  **Inconsistency**: `request.bodyUsed` 为 `false` 但流实际已空，这可能表明适配器创建了一个新的 Request 对象，但传入了一个已经空的 Stream，或者状态同步存在 Bug。

## Recommendation
建议 EdgeOne 团队检查 Next.js Runtime 适配器中关于 Request Body 的处理逻辑：
1.  确保在读取 Body（如用于 Base64 解码）后，创建一个新的 Buffer/Stream 传递给 Next.js。
2.  或者，确保使用 `stream.tee()` 来保留流的可读性。

### 立即可以进行的 POC 测试并获取详细调试信息（针对当前仓库最新提交）
```bash
curl -X POST "https://eo-umami.acofork.com/api/debug-poc" \
     -H "Content-Type: application/json" \
     -d '{"test": "hello edgeone", "timestamp": 123456}'
```
