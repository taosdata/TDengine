import { t } from 'element-ui/lib/locale';

export default {
  methods: {
    t(...args) {
      return t.apply(this, args);
    }
  }
};
