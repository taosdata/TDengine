import { defineComponent, ref } from 'vue';
import { mount } from '@vue/test-utils';
import { describe, expect, it } from 'vitest';
import RuleBlockCard from './ruleBlockCard.vue';

describe('RuleBlockCard', () => {
  it('emits remove and update events', async () => {
    const wrapper = mount(RuleBlockCard, {
      props: {
        rule: {
          id: 'rule-1',
          matches: { expr: 'topic == "foo"' }
        },
        index: 0,
        active: true,
        canMoveUp: false,
        canMoveDown: true
      }
    });

    await wrapper.get('[data-test="rule-matches-input"]').setValue('topic == "bar"');
    await wrapper.get('[data-test="rule-remove-button"]').trigger('click');

    expect(wrapper.emitted('update:rule')?.[0]).toEqual([
      {
        id: 'rule-1',
        matches: { expr: 'topic == "bar"' }
      }
    ]);
    expect(wrapper.emitted('remove')?.[0]).toEqual(['rule-1']);
  });

  it('emits reorder events when move buttons are used', async () => {
    const wrapper = mount(RuleBlockCard, {
      props: {
        rule: {
          id: 'rule-2',
          matches: { expr: 'true' }
        },
        index: 1,
        active: false,
        canMoveUp: true,
        canMoveDown: true
      }
    });

    await wrapper.get('[data-test="rule-move-up-button"]').trigger('click');
    await wrapper.get('[data-test="rule-move-down-button"]').trigger('click');

    expect(wrapper.get('[data-test="rule-move-up-button"]').attributes('aria-label')).toBe('Move rule up');
    expect(wrapper.get('[data-test="rule-move-down-button"]').attributes('aria-label')).toBe('Move rule down');
    expect(wrapper.emitted('move-up')?.[0]).toEqual(['rule-2']);
    expect(wrapper.emitted('move-down')?.[0]).toEqual(['rule-2']);
  });

  it('supports keyboard selection with Enter and Space', async () => {
    const wrapper = mount(RuleBlockCard, {
      props: {
        rule: {
          id: 'rule-3',
          matches: { expr: 'true' }
        },
        index: 2,
        active: false,
        canMoveUp: true,
        canMoveDown: false
      }
    });

    await wrapper.trigger('keydown.enter');
    await wrapper.trigger('keydown.space');

    expect(wrapper.attributes('role')).toBe('button');
    expect(wrapper.attributes('tabindex')).toBe('0');
    expect(wrapper.emitted('select')).toEqual([['rule-3'], ['rule-3']]);
  });

  it('supports parent-managed update, remove, and reorder flows', async () => {
    const Harness = defineComponent({
      components: { RuleBlockCard },
      setup() {
        const activeRuleId = ref('rule-1');
        const rules = ref([
          { id: 'rule-1', matches: { expr: 'temp > 1' } },
          { id: 'rule-2', matches: { expr: 'humidity > 1' } }
        ]);

        function updateRule(nextRule: { id: string; matches: { expr: string } }) {
          const index = rules.value.findIndex(rule => rule.id === nextRule.id);
          rules.value[index] = nextRule;
        }

        function removeRule(ruleId: string) {
          rules.value = rules.value.filter(rule => rule.id !== ruleId);
          activeRuleId.value = rules.value[0]?.id || '';
        }

        function moveRule(ruleId: string, direction: 'up' | 'down') {
          const index = rules.value.findIndex(rule => rule.id === ruleId);
          const targetIndex = direction === 'up' ? index - 1 : index + 1;
          if (index < 0 || targetIndex < 0 || targetIndex >= rules.value.length) {
            return;
          }
          const nextRules = [...rules.value];
          const [movedRule] = nextRules.splice(index, 1);
          nextRules.splice(targetIndex, 0, movedRule);
          rules.value = nextRules;
        }

        return {
          activeRuleId,
          moveRule,
          removeRule,
          rules,
          updateRule
        };
      },
      template: `
        <div>
          <RuleBlockCard
            v-for="(rule, index) in rules"
            :key="rule.id"
            :rule="rule"
            :index="index"
            :active="rule.id === activeRuleId"
            :can-move-up="index > 0"
            :can-move-down="index < rules.length - 1"
            @select="activeRuleId = $event"
            @update:rule="updateRule"
            @remove="removeRule"
            @move-up="moveRule($event, 'up')"
            @move-down="moveRule($event, 'down')"
          >
            <div v-if="rule.id === activeRuleId" :data-test="'rule-slot-' + rule.id">active content</div>
          </RuleBlockCard>
        </div>
      `
    });

    const wrapper = mount(Harness);

    await wrapper.get('[data-test="rule-matches-input"]').setValue('temp > 2');
    expect(wrapper.findAllComponents(RuleBlockCard)[0].props('rule').matches).toEqual({ expr: 'temp > 2' });

    await wrapper.findAll('[data-test="rule-move-down-button"]')[0].trigger('click');
    expect(wrapper.findAllComponents(RuleBlockCard)[0].props('rule').id).toBe('rule-2');

    await wrapper.findAllComponents(RuleBlockCard)[0].trigger('click');
    expect(wrapper.find('[data-test="rule-slot-rule-2"]').exists()).toBe(true);

    await wrapper.findAll('[data-test="rule-remove-button"]')[0].trigger('click');
    const remainingRules = wrapper.findAllComponents(RuleBlockCard).map(component => component.props('rule').id);
    expect(remainingRules).toEqual(['rule-1']);
  });
});
