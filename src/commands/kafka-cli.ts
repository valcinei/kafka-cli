import { GluegunToolbox } from 'gluegun'

module.exports = {
  name: 'kafka-cli',
  run: async (toolbox: GluegunToolbox) => {
    const { print } = toolbox

    print.warning('Welcome to Kafika CLI')
    print.divider()
    print.success('To produce events')
    print.success('$ kfk producer')
    print.success('$ kfk p')
    print.divider()
    print.warning('To consume events')
    print.success('$ kfk consumer')
    print.success('$ kfk c')
    print.divider()
    process.exit(0)
  }
}
