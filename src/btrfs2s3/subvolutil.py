from btrfsutil import SubvolumeInfo

NULL_UUID = b"\0"*16

def mkinfo(*, id:int=0, parent_id:int=0, dir_id:int=0, flags:int=0, uuid:bytes=NULL_UUID,
        parent_uuid:bytes=NULL_UUID, received_uuid:bytes=NULL_UUID,
        generation:int=0, ctransid:int=0, otransid:int=0, stransid:int=0,
        rtransid:int=0, ctime:float=0.0, otime:float=0.0, stime:float=0.0,
        rtime:float=0.0) -> SubvolumeInfo:
    return SubvolumeInfo((id, parent_id, dir_id, flags, uuid, parent_uuid,
        received_uuid, generation, ctransid, otransid, stransid, rtransid,
        ctime, otime, stime, rtime))
