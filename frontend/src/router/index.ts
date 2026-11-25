import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'
import BackfillPlansView from '@/views/BackfillPlansView.vue'
import BackfillPlanDetail from '@/views/BackfillPlanDetail.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    // 固定路径必须排在 /:moduleKey 之前，否则会被模块路由抢先匹配。
    { path: '/backfill-plans', name: 'backfill-plans', component: BackfillPlansView },
    {
      path: '/backfill-plans/:planId',
      name: 'backfill-plan-detail',
      component: BackfillPlanDetail,
      props: true,
    },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
