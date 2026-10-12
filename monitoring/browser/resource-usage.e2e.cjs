// Runs against an existing, VPN-accessible Grafana. No write credentials used.
const {chromium, expect} = require('@playwright/test');
const fs = require('node:fs/promises');
const path = require('node:path');
const base = (process.env.GRAFANA_BASE_URL || '').replace(/\/$/, '');
if (!base) throw new Error('GRAFANA_BASE_URL must point to a reachable Grafana');
const out = process.env.ARTIFACT_DIR || 'test-results/resource-monitoring';
const overview = '/d/heterocloud-cluster-resources/overview';
const panel = (page, title) => page.getByTestId('data-testid Panel header ' + title);
const slices = p => p.getByTestId('data testid Pie Chart Slice').locator('path');

async function insidePoint(slice) {
  await slice.waitFor({state:'visible'});
  // Bounding-box centers can be in the donut hole. Find a real SVG fill point,
  // then use the actual mouse; dispatching synthetic click events is not enough.
  return slice.evaluate(el => {
    const box = el.getBBox(), matrix = el.getScreenCTM();
    for (let y=box.y+3; y<box.y+box.height; y+=3)
      for (let x=box.x+3; x<box.x+box.width; x+=3)
        if (el.isPointInFill(new DOMPoint(x,y)) && el.isPointInFill(new DOMPoint(x+1,y+1))) {
          const p = new DOMPoint(x,y).matrixTransform(matrix); return {x:p.x,y:p.y};
        }
    throw new Error('No visible donut slice fill');
  });
}

async function hoverLegend(page, chart, name) {
  const names = await chart.locator('tbody tr td:nth-child(2)').allTextContents();
  const index = names.findIndex(n => n.trim() === name);
  expect(index, 'Legend contains ' + name).toBeGreaterThanOrEqual(0);
  await page.mouse.move(1,1);
  await page.waitForTimeout(200);
  const before = await page.getByText(name,{exact:true}).count();
  const point = await insidePoint(slices(chart).nth(index));
  await page.mouse.move(point.x,point.y);
  await expect.poll(() => page.getByText(name,{exact:true}).count()).toBeGreaterThan(before);
  return point;
}

(async () => {
  await fs.mkdir(out, {recursive:true});
  const browser = await chromium.launch({headless:true, args:['--no-sandbox']});
  const page = await browser.newPage({viewport:{width:1680,height:1200}});
  const evidence = {started_at:new Date().toISOString(), checks:[]};
  try {
    const response = await page.request.get(base + '/api/datasources/proxy/uid/prometheus/api/v1/query', {
      params:{query:'max by(user_id,user_name,service_id,service_name)(heterocloud_service_info)'}});
    expect(response.ok()).toBeTruthy();
    const inventory = (await response.json()).data.result.map(x => x.metric);
    expect(inventory.length).toBeGreaterThan(0);
    const users = [...new Set(inventory.map(x => x.user_id))];
    const query = async (expr, time) => {
      const r = await page.request.get(base + '/api/datasources/proxy/uid/prometheus/api/v1/query', {
        params:{query:expr, time:String(time || Math.floor(Date.now()/1000))}});
      expect(r.ok()).toBeTruthy();
      const body = await r.json(); expect(body.status).toBe('success'); return body.data.result;
    };
    const workerNodes = (await query('min by (node) (heterocloud_node_workload_pool) == 1')).map(x=>x.metric.node).sort();
    expect(workerNodes.length).toBeGreaterThan(0);
    const dashboard = (await (await page.request.get(base + '/api/dashboards/uid/heterocloud-cluster-resources')).json()).dashboard;
    for (const resource of ['CPU','メモリ','ディスク']) {
      await page.goto(base + overview, {waitUntil:'domcontentloaded'});
      const physical = panel(page, resource + ' 容量 / 物理マシン');
      await physical.scrollIntoViewIfNeeded();
      await expect(slices(physical).first()).toBeVisible({timeout:30000});
      await expect(physical).not.toContainText('No data');
      const names = await physical.locator('tbody tr').allInnerTexts();
      expect(names.length).toBeGreaterThan(0);
      const physicalNodes = (await physical.locator('tbody tr td:nth-child(2)').allTextContents()).map(x=>x.trim()).sort();
      expect(physicalNodes).toEqual(workerNodes);
      const name = (await physical.locator('tbody tr').first().locator('td').nth(1).innerText()).trim();
      await page.mouse.move(1,1);
      await page.waitForTimeout(200);
      const before = await page.getByText(name,{exact:true}).count();
      const hover = await insidePoint(slices(physical).first());
      await page.mouse.move(hover.x,hover.y);
      await expect.poll(() => page.getByText(name,{exact:true}).count()).toBeGreaterThan(before);
      await page.screenshot({path:path.join(out,resource+'-node-hover.png')});
      evidence.checks.push({resource,node_hover:true,node:name,physical_nodes:physicalNodes});
      const usage = panel(page, resource + ' 実使用量 / ユーザー' + (resource==='ディスク'?'（取得済み分）':''));
      await usage.scrollIntoViewIfNeeded();
      await expect(usage).toContainText('未使用',{timeout:30000});
      await expect(usage).toContainText('システム・未帰属');
      const timestamp = Math.floor(Date.now()/1000);
      const usagePanel = dashboard.panels.find(x=>x.title===resource+' 実使用量 / ユーザー'+(resource==='ディスク'?'（取得済み分）':''));
      const capacityPanel = dashboard.panels.find(x=>x.title===resource+' 容量 / 物理マシン');
      const parts = await Promise.all(usagePanel.targets.map(t=>query(t.expr,timestamp)));
      const capacity = (await query(capacityPanel.targets[0].expr,timestamp)).reduce((n,x)=>n+Number(x.value[1]),0);
      const chartTotal = parts.flat().reduce((n,x)=>n+Number(x.value[1]),0);
      expect(Math.abs(chartTotal-capacity)/capacity).toBeLessThan(0.000001);
      const free = await hoverLegend(page,usage,'未使用');
      await page.mouse.click(free.x,free.y);
      await expect(page).toHaveURL(/\/d\/heterocloud-cluster-resources\//);
      await page.screenshot({path:path.join(out,resource+'-capacity-denominator.png')});
      evidence.checks.push({resource,capacity_denominator:true,capacity,chart_total:chartTotal,unused_has_no_user_link:true});
      const userNames = (await usage.locator('tbody tr td:nth-child(2)').allTextContents()).map(x=>x.trim());
      const userName = userNames.find(n=>inventory.some(x=>x.user_name===n));
      expect(userName).toBeTruthy();
      const click = await hoverLegend(page,usage,userName);
      await page.mouse.click(click.x,click.y);
      await expect(page).toHaveURL(/\/d\/heterocloud-user-resources\//);
      const uid = new URL(page.url()).searchParams.get('var-user');
      expect(users).toContain(uid);
      const table = panel(page,'サービス別の実使用量と取得状態');
      await table.scrollIntoViewIfNeeded();
      for (const service of inventory.filter(x=>x.user_id===uid)) {
        await expect(table).toContainText(service.service_id,{timeout:20000});
      }
      for (const service of inventory.filter(x=>x.user_id!==uid)) {
        await expect(table).not.toContainText(service.service_id);
      }
      await page.screenshot({path:path.join(out,resource+'-service-detail.png')});
      evidence.checks.push({resource,click_drilldown:true,user:uid,services:inventory.filter(x=>x.user_id===uid).length});
    }
    // Exercise every user's filtered route, including the tiny second-user slice.
    for (const uid of users) {
      await page.goto(base+'/d/heterocloud-user-resources/usage?var-user='+encodeURIComponent(uid));
      const table=panel(page,'サービス別の実使用量と取得状態');
      await table.scrollIntoViewIfNeeded();
      for (const service of inventory) {
        if (service.user_id===uid) await expect(table).toContainText(service.service_id,{timeout:20000});
        else await expect(table).not.toContainText(service.service_id);
      }
      evidence.checks.push({user:uid,filtered_services:true});
    }
    await page.goto(base+'/');
    await expect(page.getByText('CPU 総容量（論理コア）',{exact:true})).toBeVisible({timeout:20000});
    evidence.checks.push({default_home:true});
    evidence.outcome='passed';
    console.log(JSON.stringify(evidence));
  } catch (error) {
    evidence.outcome='failed';evidence.error=error.message;
    await page.screenshot({path:path.join(out,'failure.png')});
    throw error;
  } finally {
    evidence.finished_at=new Date().toISOString();
    await fs.writeFile(path.join(out,'result.json'),JSON.stringify(evidence,null,2)+'\n');
    await browser.close();
  }
})().catch(error=>{console.error(error.message);process.exitCode=1;});
