import { mount } from '@vue/test-utils'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { ref } from 'vue'
import { explorerPropsKey, sqlProviderKey } from '../model/useExplorer'
import CustomTreeNode from './customTreeNode.vue'

vi.mock('json-big', () => ({
  default: () => ({ parse: JSON.parse, stringify: JSON.stringify }),
  parse: JSON.parse,
  jsonStringify: JSON.stringify
}))

const { confirmMock, alertMock, errorMock } = vi.hoisted(() => ({
  confirmMock: vi.fn(),
  alertMock: vi.fn(),
  errorMock: vi.fn()
}))

vi.mock('element-plus', async () => {
  const actual = await vi.importActual<typeof import('element-plus')>('element-plus')
  return {
    ...actual,
    ElMessageBox: { confirm: confirmMock, alert: alertMock },
    ElMessage: { error: errorMock }
  }
})

const deleteApi = vi.fn()
const getDataSourceUsedList = vi.fn()

function makeNode(typeName = 'database', name = 'demo') {
  return mount(CustomTreeNode, {
    props: {
      node: { label: name } as any,
      data: { name, typeName },
      defaultExpandedKeys: [],
      stableTagFilterMap: {}
    },
    global: {
      provide: {
        [explorerPropsKey as symbol]: {
          isCloud: false,
          isCommunity: false,
          customCompCallback: vi.fn(),
          database: {
            deleteApi,
            getDataSourceUsedList,
            getStructApi: vi.fn(),
            createApi: vi.fn(),
            updateApi: vi.fn(),
            isCanCreateDatabase: true
          },
          stable: {},
          table: {},
          favorite: {
            api: {
              getList: vi.fn(),
              getSharedList: vi.fn(),
              add: vi.fn(),
              edit: vi.fn(),
              addShared: vi.fn(),
              delete: vi.fn(),
              deleteShared: vi.fn()
            }
          },
          pageTitle: 'Explorer'
        },
        [sqlProviderKey as symbol]: {
          addSql: vi.fn(),
          sqlStr: ref('')
        }
      },
      stubs: {
        Info: true,
        Icon: true,
        ElDropdown: true,
        ElDropdownMenu: true,
        ElDropdownItem: true,
        ElDialog: true,
        ElTooltip: true
      }
    }
  })
}

describe('customTreeNode database delete guard', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    confirmMock.mockResolvedValue(undefined)
    deleteApi.mockResolvedValue(undefined)
  })

  it('blocks deletion and shows alert when a running task targets the database', async () => {
    getDataSourceUsedList.mockResolvedValue([
      { id: 7, name: 'mysql_sync', status: 'running', to_expand: { subject: 'demo' } }
    ])

    const wrapper = makeNode()
    await (wrapper.vm as any).del()

    expect(alertMock).toHaveBeenCalled()
    expect(confirmMock).not.toHaveBeenCalled()
    expect(deleteApi).not.toHaveBeenCalled()
  })

  it('blocks deletion when task uses targetDB field', async () => {
    getDataSourceUsedList.mockResolvedValue([
      { id: 8, name: 'sync_task', status: 'tick', targetDB: 'demo' }
    ])

    const wrapper = makeNode()
    await (wrapper.vm as any).del()

    expect(alertMock).toHaveBeenCalled()
    expect(deleteApi).not.toHaveBeenCalled()
  })

  it('proceeds to confirm when all matching tasks are stopped', async () => {
    getDataSourceUsedList.mockResolvedValue([
      { id: 9, name: 'old_sync', status: 'stopped', to_expand: { subject: 'demo' } }
    ])

    const wrapper = makeNode()
    await (wrapper.vm as any).del()

    expect(confirmMock).toHaveBeenCalled()
    expect(alertMock).not.toHaveBeenCalled()
  })

  it('proceeds to confirm when tasks target a different database', async () => {
    getDataSourceUsedList.mockResolvedValue([
      { id: 10, name: 'other_sync', status: 'running', to_expand: { subject: 'other_db' } }
    ])

    const wrapper = makeNode()
    await (wrapper.vm as any).del()

    expect(confirmMock).toHaveBeenCalled()
    expect(alertMock).not.toHaveBeenCalled()
  })

  it('shows error and blocks deletion when task query fails', async () => {
    getDataSourceUsedList.mockRejectedValue(new Error('network error'))

    const wrapper = makeNode()
    await (wrapper.vm as any).del()

    expect(errorMock).toHaveBeenCalled()
    expect(deleteApi).not.toHaveBeenCalled()
    expect(confirmMock).not.toHaveBeenCalled()
  })

  it('does not call getDataSourceUsedList for non-database nodes', async () => {
    const stableKey = 'my_stable'
    const wrapper = mount(CustomTreeNode, {
      props: {
        node: { label: stableKey, expanded: false, parent: { data: { name: 'testdb', 'node-key': 'testdb' }, level: 0, parent: null } } as any,
        data: { name: stableKey, typeName: 'stable', 'node-key': stableKey },
        defaultExpandedKeys: [],
        stableTagFilterMap: {
          [stableKey]: { advanced: { enable: false }, name: '' }
        }
      },
      global: {
        provide: {
          [explorerPropsKey as symbol]: {
            isCloud: false,
            isCommunity: false,
            customCompCallback: vi.fn(),
            database: {
              deleteApi,
              getDataSourceUsedList,
              getStructApi: vi.fn(),
              createApi: vi.fn(),
              updateApi: vi.fn(),
              isCanCreateDatabase: true
            },
            stable: {},
            table: {},
            favorite: {
              api: {
                getList: vi.fn(),
                getSharedList: vi.fn(),
                add: vi.fn(),
                edit: vi.fn(),
                addShared: vi.fn(),
                delete: vi.fn(),
                deleteShared: vi.fn()
              }
            },
            pageTitle: 'Explorer'
          },
          [sqlProviderKey as symbol]: {
            addSql: vi.fn(),
            sqlStr: ref('')
          }
        },
        stubs: {
          Info: true,
          Icon: true,
          ElDropdown: true,
          ElDropdownMenu: true,
          ElDropdownItem: true,
          ElDialog: true,
          ElTooltip: true
        }
      }
    })
    await (wrapper.vm as any).del()

    expect(getDataSourceUsedList).not.toHaveBeenCalled()
  })
})
