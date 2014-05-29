from yos.rt import BaseTasklet
from yzero import Zero, SeriesDefinition, WriteAssistant


class TestTasklet(BaseTasklet):
    def on_startup(self):
        self.zero = Zero([
                                ('192.168.224.100', 20)
                         ])
        self.wass = WriteAssistant('test', self.zero).start()
        self.wass.write(100, b'    ')
        self.wass.write(101, b'    ')
        self.wass.write(102, b'    ')
        self.wass.write(103, b'    ')
        self.wass.write(104, b'    ')
        self.wass.write(105, b'    ')
        self.wass.write(106, b'    ')        