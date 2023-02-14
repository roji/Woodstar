using System;

namespace Woodstar.Tds.Tokens;

class DoneToken : Token
{
    public DoneToken(DoneStatus status, ushort currentCommand, ulong doneRowCount)
    {
        Status = status;
        CurrentCommand = currentCommand;
        DoneRowCount = doneRowCount;
    }

    public DoneStatus Status { get; }
    public ushort CurrentCommand { get; }
    public ulong DoneRowCount { get; }

}

[Flags]
enum DoneStatus : ushort
{
    Final = 0x00,
    More =  0x1,
    Error = 0x2,
    InTransaction = 0x4,
    Count = 0x10,
    Attention = 0x20,
    ServerError = 0x100
}
