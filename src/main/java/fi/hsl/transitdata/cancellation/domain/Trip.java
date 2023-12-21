package fi.hsl.transitdata.cancellation.domain;

import java.util.Date;

public class Trip {
    long dvjId;
    long deviationCaseId;
    String routeName;
    int direction;
    String operatingDay;
    String startTime;
    String affectedDeparturesStatus;
    String deviationCasesType;

    Date AffectedDeparturesLastModified;

    public long getDvjId() {
        return dvjId;
    }

    public void setDvjId(long dvjId) {
        this.dvjId = dvjId;
    }

    public long getDeviationCaseId() {
        return deviationCaseId;
    }

    public void setDeviationCaseId(long deviationCaseId) {
        this.deviationCaseId = deviationCaseId;
    }

    public String getRouteName() {
        return routeName;
    }

    public void setRouteName(String routeName) {
        this.routeName = routeName;
    }

    public int getDirection() {
        return direction;
    }

    public void setDirection(int direction) {
        this.direction = direction;
    }

    public String getOperatingDay() {
        return operatingDay;
    }

    public void setOperatingDay(String operatingDay) {
        this.operatingDay = operatingDay;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getAffectedDeparturesStatus() {
        return affectedDeparturesStatus;
    }

    public void setAffectedDeparturesStatus(String affectedDeparturesStatus) {
        this.affectedDeparturesStatus = affectedDeparturesStatus;
    }

    public String getDeviationCasesType() {
        return deviationCasesType;
    }

    public void setDeviationCasesType(String deviationCasesType) {
        this.deviationCasesType = deviationCasesType;
    }

    public Date getAffectedDeparturesLastModified() {
        return AffectedDeparturesLastModified;
    }

    public void setAffectedDeparturesLastModified(Date affectedDeparturesLastModified) {
        AffectedDeparturesLastModified = affectedDeparturesLastModified;
    }
}
